package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mobiletoly/go-oversync/oversync"
)

// NewA1SurvivorTestServer uses the ordinary HTTP authority and an application
// trigger to expose FK-01 through both committed push effects and subsequent pull.
func NewA1SurvivorTestServer() (*TestServer, error) {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "categories" {
			generation.Tables = append(generation.Tables, table)
		}
	}
	return NewTestServer(&ServerConfig{
		AppName: "nethttp-server-example", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		SchemaGenerations: []oversync.SchemaGeneration{generation},
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			_, err := pool.Exec(ctx, `CREATE FUNCTION business.a1_survivors() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.name = 'a1-delete' THEN
                    UPDATE business.categories SET parent_id=(SELECT id FROM business.categories
                        WHERE _sync_scope_id=NEW._sync_scope_id AND name='a1-other')
                        WHERE _sync_scope_id=NEW._sync_scope_id AND parent_id=NEW.id;
                    DELETE FROM business.categories WHERE _sync_scope_id=NEW._sync_scope_id AND id=NEW.id;
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER z_a1_survivors AFTER UPDATE ON business.categories
                FOR EACH ROW EXECUTE FUNCTION business.a1_survivors()`)
			return err
		},
	})
}

// NewA2DependencyTestServer admits the existing self-FK topology with NO ACTION
// and an actual hard push limit of two. It is an isolated test authority only.
func NewA2DependencyTestServer() (*TestServer, error) {
	generation := oversync.SchemaGeneration{ID: 1}
	for _, table := range SchemaGenerationsForBusinessSchema("business")[0].Tables {
		if table.Table == "categories" {
			table.ForeignKeys[0].OnDelete = oversync.GenerationForeignKeyNoAction
			generation.Tables = append(generation.Tables, table)
		}
	}
	authority, err := NewTestServer(&ServerConfig{
		AppName: "nethttp-server-example", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxRowsPerBundle: 2, SchemaGenerations: []oversync.SchemaGeneration{generation},
		BeforeBootstrap: func(ctx context.Context, pool *pgxpool.Pool, _ string) error {
			_, err := pool.Exec(ctx, `ALTER TABLE business.categories DROP CONSTRAINT categories_parent_id_fkey;
    ALTER TABLE business.categories ADD CONSTRAINT categories_parent_id_fkey
    FOREIGN KEY(_sync_scope_id,parent_id) REFERENCES business.categories(_sync_scope_id,id)
    ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED`)
			return err
		},
	})
	if err != nil {
		return nil, err
	}
	previous := authority.HTTPServer
	var pushOrdinal atomic.Int64
	// Observe committed synthetic remote rows without adding an HTTP endpoint.
	authority.HTTPServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		previous.Config.Handler.ServeHTTP(w, r)
		if r.URL.Path == "/sync/push" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var state string
			err := authority.Pool.QueryRow(ctx, `SELECT COALESCE(jsonb_agg(to_jsonb(c) ORDER BY c._sync_scope_id,c.id),'[]'::jsonb)::text FROM business.categories c`).Scan(&state)
			if err != nil {
				fmt.Println("A2_REMOTE_STATE_ERROR")
				return
			}
			fmt.Printf("A2_REMOTE_STATE pushOrdinal=%d state=%s\n", pushOrdinal.Add(1), state)
		}
	}))
	previous.Close()
	return authority, nil

}
