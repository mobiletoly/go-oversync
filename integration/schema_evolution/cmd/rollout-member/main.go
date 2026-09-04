package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	schemaevolution "github.com/mobiletoly/go-oversync/integration/schema_evolution"
)

func main() {
	config := flag.String("config", "", "private member configuration file")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	f, err := os.Open(*config)
	if err != nil {
		fail(err)
	}
	var input schemaevolution.LocalMemberConfig
	err = schemaevolution.ReadRolloutArtifact(f, &input)
	_ = f.Close()
	if err != nil {
		fail(err)
	}
	if err = schemaevolution.ServeLocalRolloutMember(ctx, os.Getenv("DATABASE_URL"), input, os.Stdout); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
