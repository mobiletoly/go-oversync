// Local authority for the paired A1 survivor regression; no deployment entrypoint.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mobiletoly/go-oversync/integration/nethttp_server/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (result error) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if len(os.Args) == 2 && os.Args[1] == "--cleanup" {
		cleanup, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		return server.CleanupSmokeDatabase(cleanup)
	}
	var authority *server.TestServer
	var err error
	switch {
	case len(os.Args) == 1:
		authority, err = server.NewA1SurvivorTestServer()
	case len(os.Args) == 2 && os.Args[1] == "--a2-dependency":
		authority, err = server.NewA2DependencyTestServer()
	default:
		return fmt.Errorf("unsupported local fixture mode")
	}
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, authority.CloseWithError()) }()
	fmt.Printf("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=%s\n", authority.URL())
	<-ctx.Done()
	return nil
}
