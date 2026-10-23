package main

import (
	"context"
	"errors"
	"flag"
	"github.com/rychkin/Begom/internal/acceptance"
	"github.com/rychkin/Begom/internal/backup"
	"github.com/rychkin/Begom/internal/bootstrap"
	"github.com/rychkin/Begom/internal/config"
	"time"
)

func extraCommand(ctx context.Context, args []string) error {
	switch args[0] {
	case "database-init":
		return bootstrap.InitDatabase(ctx)
	case "acceptance":
		file, out, driver, inventory := ".local/client.json", "artifacts/acceptance", "compose", ""
		if e := flags("acceptance", args[1:], func(f *flag.FlagSet) {
			f.StringVar(&file, "config", file, "client profile")
			f.StringVar(&out, "output", out, "report directory")
			f.StringVar(&driver, "driver", driver, "compose, ansible or none")
			f.StringVar(&inventory, "inventory", inventory, "Ansible inventory")
		}); e != nil {
			return e
		}
		c, cfg, e := login(ctx, file)
		if e != nil {
			return e
		}
		runCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
		defer cancel()
		return acceptance.Run(runCtx, c, acceptance.Options{Client: cfg, Output: out, Driver: driver, Inventory: inventory})
	case "backup", "restore":
		file, action, snapshot, target, database := ".local/config/backup.json", "run", "", "", ""
		rest := args[1:]
		if len(rest) > 0 && rest[0][0] != '-' {
			action = rest[0]
			rest = rest[1:]
		}
		if e := flags(args[0], rest, func(f *flag.FlagSet) {
			f.StringVar(&file, "config", file, "backup profile")
			f.StringVar(&snapshot, "snapshot", snapshot, "restic snapshot ID")
			f.StringVar(&target, "target", target, "new isolated restore directory")
			f.StringVar(&database, "database", database, "new begom_restore_* database")
		}); e != nil {
			return e
		}
		var c backup.Config
		if e := config.Read(file, &c); e != nil {
			return e
		}
		if e := backup.ValidateConfig(c); e != nil {
			return e
		}
		if args[0] == "restore" {
			result, e := backup.Restore(ctx, c, snapshot, target, database)
			if e != nil {
				return e
			}
			return printJSON(result)
		}
		if action == "init" {
			return backup.Init(ctx, c)
		}
		if action != "run" {
			return errors.New("backup actions: init, run")
		}
		result, e := backup.Run(ctx, c)
		if e != nil {
			return e
		}
		return printJSON(result)
	default:
		return errors.New("unknown command")
	}
}
