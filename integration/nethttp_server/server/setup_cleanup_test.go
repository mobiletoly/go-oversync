package server

import (
	"context"
	"errors"
	"testing"
)

func TestSmokeCleanup_RejectsUnownedNames(t *testing.T) {
	for _, name := range []string{"", "postgres", "oversync_smoke_", "oversync_smoke_" + "../../unowned", "oversync_smoke_0000000000000000000000000000000Z"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("OVERSYNC_SMOKE_DATABASE_NAME", name)
			if err := CleanupSmokeDatabase(context.Background()); err == nil {
				t.Fatal("unowned database accepted")
			}
		})
	}
	if err := validateSmokeDatabaseName("oversync_smoke_0123456789abcdef0123456789abcdef"); err != nil {
		t.Fatal(err)
	}
}

func TestSmokeCleanup_ReportsFailureAndAllowsRetry(t *testing.T) {
	want := errors.New("drop failed")
	attempts := 0
	server := &TestServer{dropDatabase: func() error {
		attempts++
		if attempts == 1 {
			return want
		}
		return nil
	}}
	if err := server.CloseWithError(); !errors.Is(err, want) {
		t.Fatalf("cleanup error = %v", err)
	}
	if err := server.CloseWithError(); err != nil {
		t.Fatal(err)
	}
	if err := server.CloseWithError(); err != nil || attempts != 2 {
		t.Fatalf("successful cleanup repeated: attempts=%d err=%v", attempts, err)
	}
}
