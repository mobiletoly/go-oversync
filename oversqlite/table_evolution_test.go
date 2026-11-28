package oversqlite

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

type tableEvolutionContractVector struct {
	Format      string                           `json:"format"`
	Registered  []oversync.RegisteredTable       `json:"registered_tables"`
	Generations []oversync.SchemaGeneration      `json:"generations"`
	Proofs      []oversync.SchemaGenerationProof `json:"proofs"`
}

func tableEvolutionVector(t *testing.T) tableEvolutionContractVector {
	t.Helper()
	b, err := os.ReadFile("../conformance/table-evolution/v1/contract.json")
	require.NoError(t, err)
	var v tableEvolutionContractVector
	require.NoError(t, json.Unmarshal(b, &v))
	require.Equal(t, "oversync.table-evolution.v1", v.Format)
	return v
}
func TestTableEvolution_GenerationEdgeVectors(t *testing.T) {
	v := tableEvolutionVector(t)
	compile := func(g oversync.SchemaGeneration) oversync.SchemaGenerationContract {
		r, err := oversync.CompileSchemaGenerationRegistry(v.Registered, []oversync.SchemaGeneration{g})
		require.NoError(t, err)
		c, _ := r.Generation(g.ID)
		return c
	}
	old, target := compile(v.Generations[0]), compile(v.Generations[1])
	require.Equal(t, v.Proofs[0].Fingerprint, old.Fingerprint())
	require.Equal(t, v.Proofs[1].Fingerprint, target.Fingerprint())
	require.NoError(t, validateGenerationEvolution(old, target))
	for _, scenario := range []string{"changed_action", "old_visible_fk", "shared_type", "shared_nullability", "omit_fk_column", "omit_child_table"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(v.Generations[1])
			var next oversync.SchemaGeneration
			require.NoError(t, json.Unmarshal(raw, &next))
			next.ID = 3
			people := 0
			for i, g := range next.Tables {
				if g.Table == "people" {
					people = i
				}
			}
			prior := target
			switch scenario {
			case "changed_action":
				next.Tables[people].ForeignKeys[0].OnDelete = oversync.GenerationForeignKeyCascade
			case "old_visible_fk":
				var priorGen oversync.SchemaGeneration
				raw, _ := json.Marshal(next)
				require.NoError(t, json.Unmarshal(raw, &priorGen))
				priorGen.ID = 1
				priorGen.Tables[people].ForeignKeys = nil
				prior = compile(priorGen)
			case "shared_type":
				for i, c := range next.Tables[people].Columns {
					if c.Name == "name" {
						next.Tables[people].Columns[i].WireType = oversync.GenerationWireTypeJSON
					}
				}
			case "shared_nullability":
				for i, c := range next.Tables[people].Columns {
					if c.Name == "name" {
						next.Tables[people].Columns[i].Nullable = true
					}
				}
			case "omit_fk_column":
				next.Tables[people].ForeignKeys = nil
				var columns []oversync.GenerationColumn
				for _, c := range next.Tables[people].Columns {
					if c.Name != "project_id" {
						columns = append(columns, c)
					}
				}
				next.Tables[people].Columns = columns
			case "omit_child_table":
				var tables []oversync.GenerationTable
				for _, g := range next.Tables {
					if g.Table != "attachments" {
						tables = append(tables, g)
					}
				}
				next.Tables = tables
			}
			err := validateGenerationEvolution(prior, compile(next))
			if scenario == "omit_fk_column" || scenario == "omit_child_table" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestTableEvolution_AdditiveFKRequiresNullableChild(t *testing.T) {
	for _, tableName := range []string{"people", "attachments"} {
		for _, action := range []oversync.GenerationForeignKeyAction{oversync.GenerationForeignKeyCascade, oversync.GenerationForeignKeyNoAction} {
			for _, nullable := range []bool{false, true} {
				name := tableName + "/" + string(action) + "/required"
				if nullable {
					name = tableName + "/" + string(action) + "/nullable"
				}
				t.Run(name, func(t *testing.T) {
					v := tableEvolutionVector(t)
					for i := range v.Generations[1].Tables {
						table := &v.Generations[1].Tables[i]
						if table.Table != tableName {
							continue
						}
						table.ForeignKeys[0].OnDelete = action
						for j := range table.Columns {
							if table.Columns[j].Name == table.ForeignKeys[0].Columns[0] {
								table.Columns[j].Nullable = nullable
							}
						}
					}
					// Generic generation validation legitimately accepts a required
					// CASCADE/NO ACTION relationship; the additive edge must not.
					r, err := oversync.CompileSchemaGenerationRegistry(v.Registered, v.Generations)
					require.NoError(t, err)
					old, _ := r.Generation(1)
					target, _ := r.Generation(2)
					err = validateGenerationEvolution(old, target)
					if nullable {
						require.NoError(t, err)
					} else {
						require.ErrorContains(t, err, "requires nullable additive foreign-key column "+tableName+".")
					}
					// An already-exposed required FK is not an additive relationship.
					require.NoError(t, validateGenerationEvolution(target, target))
				})
			}
		}
	}
}

func tableVectorLocal(v tableEvolutionContractVector, index int) LocalGenerationContract {
	g := v.Generations[index]
	local := LocalGenerationContract{RegisteredTables: v.Registered, Generation: g, Proof: v.Proofs[index]}
	for _, table := range g.Tables {
		local.Tables = append(local.Tables, SyncTable{TableName: table.Table, SyncKeyColumns: []string{"id"}})
		columns := LocalTableColumns{Schema: table.Schema, Table: table.Table}
		for _, column := range table.Columns {
			codec := LocalCodecText
			if column.WireType == oversync.GenerationWireTypeUUID {
				codec = LocalCodecUUIDText
			}
			columns.Columns = append(columns.Columns, LocalColumnContract{Name: column.Name, Codec: codec})
		}
		local.Columns = append(local.Columns, columns)
	}
	return local
}
func TestTableEvolution_PhysicalFKVector(t *testing.T) {
	v := tableEvolutionVector(t)
	b, err := os.ReadFile("../conformance/table-evolution/v1/cases.json")
	require.NoError(t, err)
	var cases struct {
		Cases []struct {
			ID      string `json:"id"`
			Outcome string `json:"expected_outcome"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(b, &cases))
	found := false
	for _, c := range cases.Cases {
		if c.ID == "fk-mismatch" {
			found = true
			require.Equal(t, string(UpgradeOutcomeLocalContractMismatch), c.Outcome)
		}
	}
	require.True(t, found)
	for _, definition := range []struct {
		name, fk string
		valid    bool
	}{{"missing", "", false}, {"matching", "REFERENCES projects(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED", true}, {"wrong_action", "REFERENCES projects(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED", false}, {"plain", "REFERENCES projects(id) ON DELETE SET NULL", true}, {"not_deferrable", "REFERENCES projects(id) ON DELETE SET NULL NOT DEFERRABLE", true}} {
		t.Run(definition.name, func(t *testing.T) {
			db := openUpgradeTestDB(t)
			_, err := db.Exec(`CREATE TABLE projects(id TEXT NOT NULL PRIMARY KEY,title TEXT NOT NULL);CREATE TABLE people(id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,project_id TEXT ` + definition.fk + `);CREATE TABLE attachments(id TEXT NOT NULL PRIMARY KEY,person_id TEXT REFERENCES people(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,body TEXT NOT NULL)`)
			require.NoError(t, err)
			err = validatePhysicalLocalContract(context.Background(), db, tableVectorLocal(v, 1), nil)
			if definition.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
