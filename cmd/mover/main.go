package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"

	"github.com/ulule/mover/config"
	"github.com/ulule/mover/etl"
)

var (
	tableName string
	query     string
	path      string
	dsn       string
	verbose   bool
	version   bool
	action    string
)

func main() {
	flag.StringVar(&query, "query", "", "query to execute")
	flag.StringVar(&tableName, "table", "", "table name to export")
	flag.StringVar(&path, "path", "", "directory output")
	flag.StringVar(&dsn, "dsn", "", "database dsn")
	flag.StringVar(&action, "action", "", "action to execute")
	flag.BoolVar(&verbose, "verbose", false, "verbose logs")
	flag.BoolVar(&version, "version", false, "show version")
	flag.Parse()

	if version {
		fmt.Println("mover version", etl.Version)
		return
	}

	os.Exit(run())
}

// run executes the action and returns the exit code: 1 on any error, so that a
// caller (set -e in a script, a CI step) stops on a failed run.
func run() int {
	var (
		ctx    = context.Background()
		logger *zap.Logger
	)
	if verbose {
		logger, _ = zap.NewDevelopment()
	} else {
		logger, _ = zap.NewProduction()
	}
	//nolint:errcheck
	defer logger.Sync()

	var cfg config.Config
	if conf := os.Getenv("MOVER_CONF"); conf != "" {
		// Without its configuration, an extract would skip the anonymization.
		if err := config.Load(conf, &cfg); err != nil {
			logger.Error("unable to config", zap.Error(err), zap.String("config_path", conf))
			return 1
		}
	}

	engine, err := etl.NewEngine(ctx, cfg, dsn, logger)
	if err != nil {
		logger.Error("unable to initialize engine", zap.Error(err))
		return 1
	}

	code := 0
	switch action {
	case "extract":
		if err := engine.Extract(ctx, path, query); err != nil {
			logger.Error("unable to extract data",
				zap.Error(err),
				zap.String("table_name", tableName),
				zap.String("query", query))
			code = 1
		}
	case "load":
		if err := engine.Load(ctx, path); err != nil {
			logger.Error("unable to load data",
				zap.Error(err),
				zap.String("path", path))
			code = 1
		}
	case "describe":
		table, err := engine.Describe(ctx, tableName)
		if err != nil {
			logger.Error("unable to describe table",
				zap.Error(err),
				zap.String("table_name", tableName))
			code = 1
		} else {
			fmt.Printf("%v+\n", table)
		}
	default:
		logger.Error("unknown action", zap.String("action", action))
		code = 1
	}

	if err := engine.Shutdown(ctx); err != nil {
		logger.Error("unable to shutdown engine", zap.Error(err))
		code = 1
	}

	return code
}
