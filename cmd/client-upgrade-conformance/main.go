package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mobiletoly/go-oversync/internal/clientupgradeconformance"
)

func main() {
	root := flag.String("root", "conformance/client-upgrade/v1", "client-upgrade conformance package")
	repositoryRoot := flag.String("repository-root", ".", "go-oversync repository root")
	goAdapters := flag.Bool("go-adapters", true, "execute every mapped Go product test")
	toolchain := flag.String("toolchain", "go1.25.12", "GOTOOLCHAIN used by Go adapters")
	flag.Parse()
	if err := clientupgradeconformance.Validate(*root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *goAdapters {
		if err := clientupgradeconformance.ExecuteGoAdapters(*root, *repositoryRoot, *toolchain); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	fmt.Println("client-upgrade conformance v1: OK")
}
