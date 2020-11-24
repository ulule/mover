package main

import (
	"context"
	"flag"
	"log"
	"os"

	"github.com/ulule/mover/etl"
	"github.com/ulule/mover/etl/config"
)

var (
	tableName string
	query     string
	path      string
	dsn       string
	action    string
)

func main() {
	flag.StringVar(&query, "query", "", "query to execute")
	flag.StringVar(&tableName, "table", "", "table name to export")
	flag.StringVar(&path, "path", "", "directory output")
	flag.StringVar(&dsn, "dsn", "", "database dsn")
	flag.StringVar(&action, "action", "", "action to execute")
	flag.Parse()

	log.SetOutput(os.Stdout)

	ctx := context.Background()

	var cfg config.Config
	config.Load(os.Getenv("MOVER_CONF"), &cfg)

	engine, err := etl.NewEngine(ctx, cfg, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Shutdown(ctx)

	if action == "extract" {
		if err := engine.Extract(ctx, path, tableName, query); err != nil {
			log.Fatal(err)
		}
	} else if action == "load" {
		if err := engine.Load(ctx, path); err != nil {
			log.Fatal(err)
		}
	}
}
