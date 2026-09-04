package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
	"github.com/stretchr/testify/require"
)

func fixtureCoexistence(t *testing.T, pool *pgxpool.Pool, service *oversync.SyncService) *oversync.SyncService {
	t.Helper()
	ctx := context.Background()
	worker := fixtureWorker(pool, service)
	// The transition-only fixture drives uploads synchronously and has no live
	// consumer process whose lag is being qualified by this test.
	worker.Capacity = func(context.Context) (Capacity, error) {
		return Capacity{FreeDiskBytes: 1 << 30, Available: true, RetentionRemaining: 7 * 24 * time.Hour}, nil
	}
	require.NoError(t, worker.Run(ctx))
	proof, err := Drain(ctx, pool, []*oversync.SyncService{service}, func(context.Context) error { return nil })
	require.NoError(t, err)
	a, err := OpenAuthority(ctx, proof)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, a.Certify(ctx, 3))
	require.NoError(t, a.Apply(ctx, 4))
	require.NoError(t, a.Certify(ctx, 4))
	cfg, err := Configuration(4, 3, true)
	require.NoError(t, err)
	require.NoError(t, a.Publish(ctx, cfg))
	a.Close()
	service = bootstrapFixture(t, pool, cfg)
	require.NoError(t, ReopenServing(ctx, pool, []*oversync.SyncService{service}))
	return service
}

func TestEvolution_ReferenceUpdatePolicy(t *testing.T) {
	for _, phase := range []string{"maintaining", "coexistence"} {
		t.Run(phase, func(t *testing.T) {
			pool, service, id := fixtureMaintaining(t)
			if phase == "coexistence" {
				service = fixtureCoexistence(t, pool, service)
			}
			write := func(statement string, args ...any) error {
				if phase == "maintaining" {
					return fixtureAuthorizedWrite(t, pool, service, "scope", statement, args...)
				}
				return fixtureWrite(t, service, "scope", statement, args...)
			}
			vectors := []struct {
				sql               string
				result            [3]string
				coexist, maintain bool
			}{
				{`name='C'`, [3]string{"C|B", "C", "B"}, true, false},
				{`name='C',new_label='B'`, [3]string{"C|B", "C", "B"}, true, false},
				{`legacy_name='A|B',name='C'`, [3]string{"C|B", "C", "B"}, true, false},
				{`legacy_name='C|D',name='A',new_label='B'`, [3]string{"C|D", "C", "D"}, true, true},
				{`legacy_name='C|B',name='C'`, [3]string{"C|B", "C", "B"}, true, true},
				{`legacy_name='C|D',name='C'`, [3]string{}, false, true},
				{`legacy_name='A|B'`, [3]string{"A|B", "A", "B"}, true, true},
				{`name='A',new_label='B'`, [3]string{"A|B", "A", "B"}, true, true},
				{`name=NULL`, [3]string{}, false, false},
				{`name=''`, [3]string{}, false, false},
				{`legacy_name=NULL`, [3]string{}, false, false},
				{`legacy_name=' C|D'`, [3]string{}, false, false},
			}
			for i, v := range vectors {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					err := write(`UPDATE main.upgrade_people SET legacy_name='A|B' WHERE id=$1`, id)
					var noop *oversync.ScopeWriteNoCapturedChangesError
					require.True(t, err == nil || errors.As(err, &noop), "reset: %v", err)
					var before int64
					require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.bundle_log`).Scan(&before))
					err = write("UPDATE main.upgrade_people SET "+v.sql+" WHERE id=$1", id)
					accepted := v.maintain
					if phase == "coexistence" {
						accepted = v.coexist
					}
					var actual [3]string
					require.NoError(t, pool.QueryRow(context.Background(), `SELECT legacy_name,name,new_label FROM main.upgrade_people WHERE id=$1`, id).Scan(&actual[0], &actual[1], &actual[2]))
					if accepted {
						require.True(t, err == nil || errors.As(err, &noop), "transition rejected: %v", err)
						expected := v.result
						if phase == "maintaining" && i == 5 {
							expected = [3]string{"C|D", "C", "D"}
						}
						require.Equal(t, expected, actual)
					} else {
						require.Error(t, err)
						require.Equal(t, [3]string{"A|B", "A", "B"}, actual)
						var after int64
						require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM sync.bundle_log`).Scan(&after))
						require.Equal(t, before, after)
					}
				})
			}
		})
	}
}

func TestEvolution_ReferenceInsertAndHistoricalPolicy(t *testing.T) {
	pool, service, id := fixtureMaintaining(t)
	require.NoError(t, fixtureAuthorizedWrite(t, pool, service, "scope", `UPDATE main.upgrade_people SET name='A' WHERE id=$1`, id))
	vectors := []struct {
		columns, values   string
		maintain, coexist bool
	}{
		{"legacy_name", "'A|B'", true, true},
		{"legacy_name,name,new_label", "'A|B',NULL,NULL", true, true},
		{"name,new_label", "'A','B'", false, true},
		{"legacy_name,name,new_label", "NULL,'A','B'", false, true},
		{"name", "'A'", false, false},
		{"legacy_name,name", "'A|B','A'", true, false},
		{"legacy_name,new_label", "'A|B','B'", true, false},
		{"legacy_name,name,new_label", "'A|B','A','B'", true, true},
		{"legacy_name,name,new_label", "'A|B','C','B'", false, false},
		{"legacy_name", "'A||B'", false, false},
		{"legacy_name", "'A|B ' ", false, false},
		{"legacy_name", "'é|B'", false, false},
		{"legacy_name", "''", false, false},
	}
	for _, phase := range []string{"maintaining", "coexistence"} {
		if phase == "coexistence" {
			service = fixtureCoexistence(t, pool, service)
		}
		write := func(statement string, args ...any) error {
			if phase == "maintaining" {
				return fixtureAuthorizedWrite(t, pool, service, "scope", statement, args...)
			}
			return fixtureWrite(t, service, "scope", statement, args...)
		}
		for i, v := range vectors {
			t.Run(fmt.Sprintf("%s/%d", phase, i), func(t *testing.T) {
				key := uuid.New()
				err := write(`INSERT INTO main.upgrade_people(_sync_scope_id,id,`+v.columns+`) VALUES('scope',$1,`+v.values+`)`, key)
				accepted := v.maintain
				if phase == "coexistence" {
					accepted = v.coexist
				}
				if accepted {
					require.NoError(t, err)
					var legacy, name, label string
					require.NoError(t, pool.QueryRow(context.Background(), `SELECT legacy_name,name,new_label FROM main.upgrade_people WHERE id=$1`, key).Scan(&legacy, &name, &label))
					require.Equal(t, []string{"A|B", "A", "B"}, []string{legacy, name, label})
					if phase == "maintaining" {
						require.NoError(t, write(`DELETE FROM main.upgrade_people WHERE id=$1`, key))
					}
				} else {
					require.Error(t, err)
				}
			})
		}
	}
}
