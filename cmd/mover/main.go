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
		if err := config.Load(conf, &cfg); err != nil {
			logger.Error("unable to config", zap.Error(err), zap.String("config_path", conf))
		}
	}

	engine, err := etl.NewEngine(ctx, cfg, dsn, logger)
	if err != nil {
		logger.Error("unable to initialize engine", zap.Error(err))
		return
	}
	defer func() {
		if err := engine.Shutdown(ctx); err != nil {
			logger.Error("unable to shutdown engine", zap.Error(err))
		}
	}()

	switch action {
	case "extract":
		if err := engine.Extract(ctx, path, query); err != nil {
			logger.Error("unable to extract data",
				zap.Error(err),
				zap.String("table_name", tableName),
				zap.String("query", query))
		}
	case "load":
		if err := engine.Load(ctx, path); err != nil {
			logger.Error("unable to load data",
				zap.Error(err),
				zap.String("path", path))
		}
	case "describe":
		table, err := engine.Describe(ctx, tableName)
		if err != nil {
			logger.Error("unable to describe table",
				zap.Error(err),
				zap.String("table_name", tableName))
		}

		fmt.Printf("%v+\n", table)
	}
}
