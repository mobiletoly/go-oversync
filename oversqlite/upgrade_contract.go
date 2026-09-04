package oversqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/mobiletoly/go-oversync/internal/jcs"
	"github.com/mobiletoly/go-oversync/oversync"
)

type upgradeQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func cloneUpgradePlan(plan UpgradePlan) UpgradePlan {
	clone := plan
	clone.LocalOnlyTables = append([]string(nil), plan.LocalOnlyTables...)
	return clone
}

func cloneLocalGenerationContract(local LocalGenerationContract) LocalGenerationContract {
	clone := local
	clone.RegisteredTables = append([]oversync.RegisteredTable(nil), local.RegisteredTables...)
	for idx := range clone.RegisteredTables {
		clone.RegisteredTables[idx].SyncKeyColumns = append([]string(nil), local.RegisteredTables[idx].SyncKeyColumns...)
	}
	clone.Generation.Tables = append([]oversync.GenerationTable(nil), local.Generation.Tables...)
	for tableIdx := range clone.Generation.Tables {
		table := &clone.Generation.Tables[tableIdx]
		source := local.Generation.Tables[tableIdx]
		table.Columns = append([]oversync.GenerationColumn(nil), source.Columns...)
		table.ForeignKeys = append([]oversync.GenerationForeignKey(nil), source.ForeignKeys...)
		for fkIdx := range table.ForeignKeys {
			table.ForeignKeys[fkIdx].Columns = append([]string(nil), source.ForeignKeys[fkIdx].Columns...)
			table.ForeignKeys[fkIdx].ReferencedColumns = append([]string(nil), source.ForeignKeys[fkIdx].ReferencedColumns...)
		}
	}
	clone.Tables = append([]SyncTable(nil), local.Tables...)
	for idx := range clone.Tables {
		clone.Tables[idx].SyncKeyColumns = append([]string(nil), local.Tables[idx].SyncKeyColumns...)
	}
	clone.Columns = append([]LocalTableColumns(nil), local.Columns...)
	for idx := range clone.Columns {
		clone.Columns[idx].Columns = append([]LocalColumnContract(nil), local.Columns[idx].Columns...)
	}
	return clone
}

func validateUpgradePlan(plan UpgradePlan) (string, error) {
	if strings.TrimSpace(plan.PlanID) == "" || plan.PlanID != strings.TrimSpace(plan.PlanID) {
		return "", fmt.Errorf("plan_id must be an exact nonempty value")
	}
	if !validUpgradeMode(plan.Mode) {
		return "", fmt.Errorf("unsupported upgrade mode %q", plan.Mode)
	}
	if plan.DatabaseID == "" || plan.ServiceID == "" || plan.DatabaseID != strings.TrimSpace(plan.DatabaseID) || plan.ServiceID != strings.TrimSpace(plan.ServiceID) {
		return "", fmt.Errorf("database_id and service_id must be exact nonempty values")
	}
	if plan.InstalledVersion < 0 || plan.TargetVersion < 0 || plan.TargetVersion < plan.InstalledVersion {
		return "", fmt.Errorf("local versions must be non-negative and nondecreasing")
	}
	if plan.TargetGenerationID < 1 || (plan.Mode != UpgradeModeFresh && plan.InstalledGenerationID < 1) {
		return "", fmt.Errorf("generation IDs must select existing positive generations")
	}
	if plan.Migrator == nil {
		return "", fmt.Errorf("local migrator is required")
	}
	if plan.Mode == UpgradeModeFresh {
		if plan.InstalledVersion != 0 || plan.UserID != "" {
			return "", fmt.Errorf("fresh mode requires version zero and anonymous identity")
		}
	} else if plan.UserID != strings.TrimSpace(plan.UserID) || (plan.Mode == UpgradeModeGeneration && plan.UserID == "") {
		return "", fmt.Errorf("generation mode requires an exact nonempty user_id; local-only user_id must be exact")
	}

	if plan.Mode == UpgradeModeGeneration {
		if plan.TargetGenerationID <= plan.InstalledGenerationID {
			return "", fmt.Errorf("target generation must be strictly greater than installed generation")
		}
	}
	if plan.Mode == UpgradeModeLocalOnly && plan.TargetGenerationID != plan.InstalledGenerationID {
		return "", fmt.Errorf("local-only migration requires the exact same generation ID")
	}
	if plan.Mode == UpgradeModeLocalOnly && plan.TargetVersion <= plan.InstalledVersion {
		return "", fmt.Errorf("local-only migration must strictly increase the local version")
	}
	if plan.Mode == UpgradeModeFresh && plan.TargetVersion == 0 {
		return "", fmt.Errorf("fresh mode requires a positive target version")
	}

	return digestUpgradePlan(plan, nil, nil, nil, nil)
}

func compileCompleteLocalContract(label string, local LocalGenerationContract) (*compiledLocalContract, error) {
	compiled, err := compileLocalContract(local)
	if err == nil {
		err = requireCompleteLocalContract(local, compiled)
	}
	if err != nil {
		return nil, fmt.Errorf("%s contract: %w", label, err)
	}
	return compiled, nil
}

type compiledLocalContract struct {
	proof       oversync.SchemaGenerationProof
	contract    oversync.SchemaGenerationContract
	localTables []oversync.GenerationTableContract
	codecs      map[string]map[string]LocalColumnCodec
}

func requireCompleteLocalContract(local LocalGenerationContract, compiled *compiledLocalContract) error {
	if compiled == nil || len(local.Tables) != len(compiled.contract.Tables()) {
		return fmt.Errorf("upgrade contract must cover every generation table")
	}
	return nil
}

func compileLocalContract(local LocalGenerationContract) (*compiledLocalContract, error) {
	contract, proof, err := compileDeclaredGeneration(local)
	if err != nil {
		return nil, err
	}
	configuredTables, err := compileConfiguredSyncTables(local.Tables, len(contract.Tables()))
	if err != nil {
		return nil, err
	}
	codecs, err := compileLocalCodecs(local.Columns)
	if err != nil {
		return nil, err
	}
	if len(codecs) != len(configuredTables) {
		return nil, fmt.Errorf("local codec table set does not match local sync tables")
	}
	contractByTable := generationTablesByName(contract.Tables())
	localTables, err := validateConfiguredLocalTables(local.Tables, contractByTable, codecs)
	if err != nil {
		return nil, err
	}
	if err := validateLocalForeignKeyClosure(localTables, codecs); err != nil {
		return nil, err
	}
	return &compiledLocalContract{proof: proof, contract: contract, localTables: localTables, codecs: codecs}, nil
}

func compileDeclaredGeneration(local LocalGenerationContract) (oversync.SchemaGenerationContract, oversync.SchemaGenerationProof, error) {
	registry, err := oversync.CompileSchemaGenerationRegistry(local.RegisteredTables, []oversync.SchemaGeneration{local.Generation})
	if err != nil {
		return oversync.SchemaGenerationContract{}, oversync.SchemaGenerationProof{}, err
	}
	contract, ok := registry.Generation(local.Generation.ID)
	if !ok {
		return oversync.SchemaGenerationContract{}, oversync.SchemaGenerationProof{}, fmt.Errorf("compiled generation is missing")
	}
	proof := oversync.SchemaGenerationProof{ID: contract.ID(), Fingerprint: contract.Fingerprint()}
	if local.Proof != proof {
		return oversync.SchemaGenerationContract{}, oversync.SchemaGenerationProof{}, fmt.Errorf("supplied proof does not match compiled generation contract")
	}
	return contract, proof, nil
}

func compileConfiguredSyncTables(tables []SyncTable, generationTableCount int) (map[string]SyncTable, error) {
	if len(tables) == 0 || len(tables) > generationTableCount {
		return nil, fmt.Errorf("local sync table set must be a nonempty generation subset")
	}
	configuredTables := make(map[string]SyncTable, len(tables))
	for _, table := range tables {
		name := table.TableName
		if name == "" || name != strings.ToLower(name) || name != strings.TrimSpace(name) {
			return nil, fmt.Errorf("local table name %q is not canonical", name)
		}
		if _, exists := configuredTables[name]; exists {
			return nil, fmt.Errorf("duplicate local table %q", name)
		}
		configuredTables[name] = table
	}
	return configuredTables, nil
}

// validateConfiguredGenerationSelection validates the caller's table/key selection using only
// discovery-owned metadata. It is deliberately usable before target DDL exists, so an invalid
// selection cannot survive until the U0 freeze has already committed.
func validateConfiguredGenerationSelection(tables []SyncTable, contract oversync.SchemaGenerationContract) error {
	configured, err := compileConfiguredSyncTables(tables, len(contract.Tables()))
	if err != nil {
		return err
	}
	if len(configured) != len(contract.Tables()) {
		return fmt.Errorf("target local sync table selection must cover every discovered generation table")
	}
	byName := generationTablesByName(contract.Tables())
	for name, selected := range configured {
		candidates := byName[name]
		if len(candidates) != 1 {
			return fmt.Errorf("local sync table %q must identify one discovered generation table", name)
		}
		keys, keyErr := normalizedSyncKeyColumns(selected)
		if keyErr != nil || !stringSlicesEqual(keys, candidates[0].SyncKeyColumns) {
			return fmt.Errorf("local sync keys for %s.%s do not match generation", candidates[0].Schema, candidates[0].Table)
		}
	}
	return nil
}

func compileLocalCodecs(tables []LocalTableColumns) (map[string]map[string]LocalColumnCodec, error) {
	codecs := make(map[string]map[string]LocalColumnCodec, len(tables))
	for _, table := range tables {
		key := table.Schema + "." + table.Table
		if _, exists := codecs[key]; exists {
			return nil, fmt.Errorf("duplicate local codec table %q", key)
		}
		byColumn := make(map[string]LocalColumnCodec, len(table.Columns))
		for _, column := range table.Columns {
			if column.Name == "" || column.Name != strings.ToLower(column.Name) || column.Name != strings.TrimSpace(column.Name) {
				return nil, fmt.Errorf("local column name %q is not canonical", column.Name)
			}
			if _, exists := byColumn[column.Name]; exists {
				return nil, fmt.Errorf("duplicate local codec column %q", column.Name)
			}
			byColumn[column.Name] = column.Codec
		}
		codecs[key] = byColumn
	}
	return codecs, nil
}

func generationTablesByName(tables []oversync.GenerationTableContract) map[string][]oversync.GenerationTableContract {
	contractByTable := make(map[string][]oversync.GenerationTableContract, len(tables))
	for _, table := range tables {
		contractByTable[table.Table] = append(contractByTable[table.Table], table)
	}
	return contractByTable
}

func validateConfiguredLocalTables(configuredTables []SyncTable, contractByTable map[string][]oversync.GenerationTableContract, codecs map[string]map[string]LocalColumnCodec) ([]oversync.GenerationTableContract, error) {
	localTables := make([]oversync.GenerationTableContract, 0, len(configuredTables))
	for _, configured := range configuredTables {
		candidates := contractByTable[configured.TableName]
		if len(candidates) == 0 {
			return nil, fmt.Errorf("local sync table %q is absent from generation", configured.TableName)
		}
		var table oversync.GenerationTableContract
		matches := 0
		for _, candidate := range candidates {
			if _, ok := codecs[candidate.Schema+"."+candidate.Table]; ok {
				table = candidate
				matches++
			}
		}
		if matches != 1 {
			return nil, fmt.Errorf("local sync table %q must have one exact schema-qualified codec declaration", configured.TableName)
		}
		localTables = append(localTables, table)
		keys, err := normalizedSyncKeyColumns(configured)
		if err != nil || !stringSlicesEqual(keys, table.SyncKeyColumns) {
			return nil, fmt.Errorf("local sync keys for %s.%s do not match generation", table.Schema, table.Table)
		}
		byColumn, ok := codecs[table.Schema+"."+table.Table]
		if !ok || len(byColumn) != len(table.Columns) {
			return nil, fmt.Errorf("local codec set for %s.%s is incomplete", table.Schema, table.Table)
		}
		for _, column := range table.Columns {
			codec, ok := byColumn[column.Name]
			if !ok || !codecMatchesWireType(codec, column.WireType) {
				return nil, fmt.Errorf("local codec for %s.%s.%s does not match %s", table.Schema, table.Table, column.Name, column.WireType)
			}
		}
	}
	return localTables, nil
}

func validateLocalForeignKeyClosure(localTables []oversync.GenerationTableContract, codecs map[string]map[string]LocalColumnCodec) error {
	for _, table := range localTables {
		childCodecs := codecs[table.Schema+"."+table.Table]
		for _, foreignKey := range table.ForeignKeys {
			parentCodecs := codecs[foreignKey.ReferencedSchema+"."+foreignKey.ReferencedTable]
			if parentCodecs == nil {
				return fmt.Errorf("local sync tables are not FK-closed: %s references %s", table.Table, foreignKey.ReferencedTable)
			}
			for idx := range foreignKey.Columns {
				childCodec := childCodecs[foreignKey.Columns[idx]]
				parentCodec := parentCodecs[foreignKey.ReferencedColumns[idx]]
				if childCodec != parentCodec {
					return fmt.Errorf("local foreign-key representation for %s.%s must match %s.%s", table.Table, foreignKey.Columns[idx], foreignKey.ReferencedTable, foreignKey.ReferencedColumns[idx])
				}
			}
		}
	}
	return nil
}

func codecMatchesWireType(codec LocalColumnCodec, wire oversync.GenerationWireType) bool {
	switch wire {
	case oversync.GenerationWireTypeBoolean:
		return codec == LocalCodecBooleanInteger
	case oversync.GenerationWireTypeInt64:
		return codec == LocalCodecInt64Integer
	case oversync.GenerationWireTypeDecimal:
		return codec == LocalCodecDecimalText
	case oversync.GenerationWireTypeFloat64:
		return codec == LocalCodecFloat64Real
	case oversync.GenerationWireTypeText:
		return codec == LocalCodecText
	case oversync.GenerationWireTypeBytes:
		return codec == LocalCodecBytesBlob
	case oversync.GenerationWireTypeUUID:
		return codec == LocalCodecUUIDText || codec == LocalCodecUUIDBlob
	case oversync.GenerationWireTypeJSON:
		return codec == LocalCodecJSONText
	default:
		return false
	}
}

// validateGenerationEvolution allows view omission and additive tables/FKs.
// Shared physical column/key/FK semantics remain immutable. The upgrade journal
// still settles all old intent before invoking the caller's destructive DDL.
func validateGenerationEvolution(old, target oversync.SchemaGenerationContract) error {
	oldTables := make(map[string]oversync.GenerationTableContract)
	for _, table := range old.Tables() {
		oldTables[table.Schema+"."+table.Table] = table
	}
	for _, table := range target.Tables() {
		prior, ok := oldTables[table.Schema+"."+table.Table]
		if ok {
			if !stringSlicesEqual(prior.SyncKeyColumns, table.SyncKeyColumns) {
				return fmt.Errorf("generation evolution requires unchanged sync key identities")
			}
			if err := validateSharedGenerationColumns(prior, table); err != nil {
				return err
			}
		}
		if err := validateAdditiveGenerationFKs(prior, table); err != nil {
			return err
		}
	}
	return nil
}

func generationColumnMap(table oversync.GenerationTableContract) map[string]oversync.GenerationColumn {
	columns := make(map[string]oversync.GenerationColumn, len(table.Columns))
	for _, c := range table.Columns {
		columns[c.Name] = c
	}
	return columns
}

func validateSharedGenerationColumns(old, target oversync.GenerationTableContract) error {
	columns := generationColumnMap(old)
	for _, column := range target.Columns {
		if prior, ok := columns[column.Name]; ok && (prior.WireType != column.WireType || prior.Nullable != column.Nullable) {
			return fmt.Errorf("generation evolution changed shared column semantics for %s.%s", target.Table, column.Name)
		}
	}
	return nil
}

func validateAdditiveGenerationFKs(old, target oversync.GenerationTableContract) error {
	oldColumns, targetColumns := generationColumnMap(old), generationColumnMap(target)
	for _, fk := range old.ForeignKeys {
		visible := true
		for _, name := range fk.Columns {
			if _, ok := targetColumns[name]; !ok {
				visible = false
			}
		}
		if visible && !containsGenerationFK(target.ForeignKeys, fk) {
			return fmt.Errorf("generation evolution changed an existing visible foreign key on %s", old.Table)
		}
	}
	for _, fk := range target.ForeignKeys {
		if containsGenerationFK(old.ForeignKeys, fk) {
			continue
		}
		for _, name := range fk.Columns {
			if _, ok := oldColumns[name]; ok {
				return fmt.Errorf("generation evolution added a foreign key to an old visible column on %s", old.Table)
			}
			if !targetColumns[name].Nullable {
				return fmt.Errorf("generation evolution requires nullable additive foreign-key column %s.%s", target.Table, name)
			}
		}
	}
	return nil
}

func containsGenerationFK(set []oversync.GenerationForeignKey, want oversync.GenerationForeignKey) bool {
	for _, fk := range set {
		if foreignKeySetsEqual([]oversync.GenerationForeignKey{fk}, []oversync.GenerationForeignKey{want}) {
			return true
		}
	}
	return false
}

func foreignKeySetsEqual(left, right []oversync.GenerationForeignKey) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		l, r := left[idx], right[idx]
		if l.ReferencedSchema != r.ReferencedSchema || l.ReferencedTable != r.ReferencedTable ||
			l.OnDelete != r.OnDelete || l.OnUpdate != r.OnUpdate ||
			!stringSlicesEqual(l.Columns, r.Columns) || !stringSlicesEqual(l.ReferencedColumns, r.ReferencedColumns) {
			return false
		}
	}
	return true
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for idx := range left {
		if left[idx] != right[idx] {
			return false
		}
	}
	return true
}

func digestUpgradePlan(
	plan UpgradePlan,
	oldProof *oversync.SchemaGenerationProof,
	targetProof *oversync.SchemaGenerationProof,
	oldTables []SyncTable,
	targetTables []SyncTable,
) (string, error) {
	localOnly, err := validateLocalOnlyTableScope(plan.LocalOnlyTables)
	if err != nil {
		return "", err
	}
	proofValue := func(proof *oversync.SchemaGenerationProof) any {
		if proof == nil {
			return nil
		}
		return []any{fmt.Sprint(proof.ID), proof.Fingerprint}
	}
	tableValue := func(tables []SyncTable) []any {
		ordered := cloneSyncTables(tables)
		sort.Slice(ordered, func(i, j int) bool { return ordered[i].TableName < ordered[j].TableName })
		result := make([]any, 0, len(ordered))
		for _, table := range ordered {
			keys, keyErr := normalizedSyncKeyColumns(table)
			if keyErr != nil {
				result = append(result, []any{table.TableName, "invalid"})
				continue
			}
			result = append(result, []any{table.TableName, keys})
		}
		return result
	}
	canonical, err := jcs.Marshal([]any{
		"oversqlite.upgrade-plan.v1", plan.PlanID, string(plan.Mode), plan.DatabaseID, plan.ServiceID, plan.UserID,
		fmt.Sprint(plan.InstalledGenerationID), fmt.Sprint(plan.TargetGenerationID),
		fmt.Sprint(plan.InstalledVersion), fmt.Sprint(plan.TargetVersion), localOnly,
		proofValue(oldProof), proofValue(targetProof), tableValue(oldTables), tableValue(targetTables),
	})
	if err != nil {
		return "", fmt.Errorf("canonicalize upgrade plan: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func validateLocalOnlyTableScope(tableNames []string, contracts ...*compiledLocalContract) ([]string, error) {
	localOnly := append([]string(nil), tableNames...)
	sort.Strings(localOnly)
	synchronized := make(map[string]struct{})
	for _, compiled := range contracts {
		if compiled == nil {
			continue
		}
		for _, table := range compiled.localTables {
			synchronized[table.Table] = struct{}{}
		}
	}
	for idx, table := range localOnly {
		if table == "" || table != strings.ToLower(table) || table != strings.TrimSpace(table) || strings.HasPrefix(table, "_sync_") {
			return nil, fmt.Errorf("local-only table %q is invalid", table)
		}
		if idx > 0 && localOnly[idx-1] == table {
			return nil, fmt.Errorf("duplicate local-only table %q", table)
		}
		if _, overlaps := synchronized[table]; overlaps {
			return nil, fmt.Errorf("local-only table %q is also a synchronized table", table)
		}
	}
	return localOnly, nil
}

func validatePhysicalLocalContract(ctx context.Context, q upgradeQueryer, local LocalGenerationContract, localOnly []string) error {
	compiled, err := compileLocalContract(local)
	if err != nil {
		return err
	}
	contractByTable := make(map[string]oversync.GenerationTableContract, len(compiled.localTables))
	for _, table := range compiled.localTables {
		contractByTable[table.Table] = table
	}
	for _, configured := range local.Tables {
		table := contractByTable[configured.TableName]
		if err := validatePhysicalLocalTable(ctx, q, table, compiled.codecs[table.Schema+"."+table.Table]); err != nil {
			return err
		}
	}
	return validatePhysicalApplicationTableScope(ctx, q, local.Tables, localOnly)
}

func validatePhysicalApplicationTableScope(ctx context.Context, q upgradeQueryer, managed []SyncTable, localOnly []string) error {
	canonicalLocalOnly, err := validateLocalOnlyTableScope(localOnly)
	if err != nil {
		return err
	}
	allowedTables := make(map[string]struct{}, len(canonicalLocalOnly)+len(managed))
	for _, table := range managed {
		allowedTables[table.TableName] = struct{}{}
	}
	for _, name := range canonicalLocalOnly {
		if _, overlaps := allowedTables[name]; overlaps {
			return fmt.Errorf("local-only table %q is also a synchronized table", name)
		}
		allowedTables[name] = struct{}{}
	}
	rows, err := q.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND substr(name, 1, 7) <> 'sqlite_' AND substr(name, 1, 6) <> '_sync_' ORDER BY name`)
	if err != nil {
		return fmt.Errorf("inspect local application tables: %w", err)
	}
	defer rows.Close()
	observedTables := make(map[string]struct{}, len(allowedTables))
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return err
		}
		if _, ok := allowedTables[name]; !ok {
			return fmt.Errorf("undeclared application table %q", name)
		}
		observedTables[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for name := range allowedTables {
		if _, ok := observedTables[name]; !ok {
			return fmt.Errorf("declared application table %q is missing", name)
		}
	}
	return nil
}

func validatePhysicalLocalTable(ctx context.Context, q upgradeQueryer, table oversync.GenerationTableContract, codecs map[string]LocalColumnCodec) error {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_xinfo(%s)`, quoteIdent(table.Table)))
	if err != nil {
		return fmt.Errorf("inspect local table %s: %w", table.Table, err)
	}
	defer rows.Close()
	type physicalColumn struct {
		name, declared string
		notNull, pk    int
		hidden         int
	}
	physical := make(map[string]physicalColumn)
	for rows.Next() {
		var cid int
		var col physicalColumn
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &col.name, &col.declared, &col.notNull, &defaultValue, &col.pk, &col.hidden); err != nil {
			return err
		}
		physical[col.name] = col
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(physical) != len(table.Columns) {
		return fmt.Errorf("local table %s has a non-generation column surface", table.Table)
	}
	keySet := make(map[string]int, len(table.SyncKeyColumns))
	for idx, key := range table.SyncKeyColumns {
		keySet[key] = idx + 1
	}
	for _, column := range table.Columns {
		actual, ok := physical[column.Name]
		if !ok {
			return fmt.Errorf("local table %s is missing column %s", table.Table, column.Name)
		}
		if actual.hidden != 0 {
			return fmt.Errorf("local table %s column %s is generated or hidden", table.Table, column.Name)
		}
		if column.Nullable == (actual.notNull != 0 || actual.pk != 0) {
			return fmt.Errorf("local nullability mismatch for %s.%s", table.Table, column.Name)
		}
		if expectedPK := keySet[column.Name]; actual.pk != expectedPK {
			return fmt.Errorf("local key mismatch for %s.%s", table.Table, column.Name)
		}
		if !codecMatchesAffinity(codecs[column.Name], sqliteDeclaredAffinity(actual.declared)) {
			return fmt.Errorf("local storage mismatch for %s.%s", table.Table, column.Name)
		}
	}
	return validatePhysicalForeignKeys(ctx, q, table)
}

func sqliteDeclaredAffinity(declared string) string {
	upper := strings.ToUpper(declared)
	switch {
	case strings.Contains(upper, "INT"):
		return "INTEGER"
	case strings.Contains(upper, "CHAR"), strings.Contains(upper, "CLOB"), strings.Contains(upper, "TEXT"):
		return "TEXT"
	case strings.Contains(upper, "BLOB") || upper == "":
		return "BLOB"
	case strings.Contains(upper, "REAL"), strings.Contains(upper, "FLOA"), strings.Contains(upper, "DOUB"):
		return "REAL"
	default:
		return "NUMERIC"
	}
}

func codecMatchesAffinity(codec LocalColumnCodec, affinity string) bool {
	switch codec {
	case LocalCodecBooleanInteger, LocalCodecInt64Integer:
		return affinity == "INTEGER"
	case LocalCodecDecimalText, LocalCodecText, LocalCodecUUIDText, LocalCodecJSONText:
		return affinity == "TEXT"
	case LocalCodecFloat64Real:
		return affinity == "REAL"
	case LocalCodecBytesBlob, LocalCodecUUIDBlob:
		return affinity == "BLOB"
	default:
		return false
	}
}

func validatePhysicalForeignKeys(ctx context.Context, q upgradeQueryer, table oversync.GenerationTableContract) error {
	rows, err := q.QueryContext(ctx, fmt.Sprintf(`PRAGMA foreign_key_list(%s)`, quoteIdent(table.Table)))
	if err != nil {
		return err
	}
	defer rows.Close()
	type fk struct {
		refTable, onUpdate, onDelete string
		from, to                     []string
	}
	byID := make(map[int]*fk)
	ids := make([]int, 0)
	for rows.Next() {
		var id, seq int
		var refTable, from, to, onUpdate, onDelete string
		var match string
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return err
		}
		if !strings.EqualFold(match, "NONE") {
			return fmt.Errorf("local foreign key for %s must use MATCH SIMPLE", table.Table)
		}
		item := byID[id]
		if item == nil {
			if seq != 0 {
				return fmt.Errorf("local foreign-key sequence is not contiguous for %s", table.Table)
			}
			item = &fk{refTable: refTable, onUpdate: onUpdate, onDelete: onDelete}
			byID[id] = item
			ids = append(ids, id)
		} else if seq != len(item.from) || item.refTable != refTable || item.onUpdate != onUpdate || item.onDelete != onDelete {
			return fmt.Errorf("local foreign-key sequence is inconsistent for %s", table.Table)
		}
		item.from = append(item.from, from)
		item.to = append(item.to, to)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	sort.Ints(ids)
	actual := make([]fk, 0, len(ids))
	for _, id := range ids {
		actual = append(actual, *byID[id])
	}
	if len(actual) != len(table.ForeignKeys) {
		return fmt.Errorf("local foreign-key set mismatch for %s", table.Table)
	}
	used := make([]bool, len(actual))
	for _, expected := range table.ForeignKeys {
		found := false
		for idx, item := range actual {
			if !used[idx] && item.refTable == expected.ReferencedTable &&
				stringSlicesEqual(item.from, expected.Columns) && stringSlicesEqual(item.to, expected.ReferencedColumns) &&
				strings.EqualFold(strings.ReplaceAll(item.onDelete, " ", "_"), string(expected.OnDelete)) &&
				strings.EqualFold(strings.ReplaceAll(item.onUpdate, " ", "_"), string(expected.OnUpdate)) {
				found = true
				used[idx] = true
				break
			}
		}
		if !found {
			return fmt.Errorf("local foreign-key contract mismatch for %s", table.Table)
		}
	}
	return nil
}
