package main

import (
	"fmt"
	"os"

	"atomkv"
)

const dbPath = "atomkv.db"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	db, err := atomkv.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "error loading db: %v\n", err)
		os.Exit(1)
	}

	switch os.Args[1] {
	case "set":
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: atomkv set <key> <value>")
			os.Exit(1)
		}
		if err := db.Set(os.Args[2], os.Args[3]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("OK")

	case "get":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: atomkv get <key>")
			os.Exit(1)
		}
		val, err := db.Get(os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(val)

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: atomkv <command> [args]")
	fmt.Fprintln(os.Stderr, "  set <key> <value>  Store a key-value pair")
	fmt.Fprintln(os.Stderr, "  get <key>          Retrieve a value by key")
}
