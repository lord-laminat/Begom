package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"github.com/rychkin/Begom/internal/nodecontrol"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	if mode == "ssh" {
		switch os.Getenv("SSH_ORIGINAL_COMMAND") {
		case "begom-node-helper console":
			mode = "console"
		case "begom-node-helper rpc":
			mode = "rpc"
		default:
			os.Exit(1)
		}
	}
	var e error
	switch mode {
	case "serve":
		f := flag.NewFlagSet("serve", flag.ExitOnError)
		cfg := f.String("config", "/config/agent.json", "agent configuration")
		listen := f.String("listen", ":2222", "SSH listen address")
		host := f.String("host-key", "/run/secrets/ssh-host.key", "SSH host key")
		authorized := f.String("authorized-key", "/run/secrets/ssh-client.pub", "authorized coordinator key")
		f.Parse(os.Args[2:])
		rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		supervisor := &nodecontrol.Supervisor{Context: rootCtx, AgentPath: "/usr/local/bin/agent", ConfigPath: *cfg, UpdateRoot: "/updates", PackageKey: "/run/secrets/package.pub"}
		e = supervisor.Serve(rootCtx, *listen, *host, *authorized)
	case "console":
		e = nodecontrol.Console(ctx, os.Stdin, os.Stdout)
	case "privileged":
		if len(os.Args) != 3 {
			os.Exit(1)
		}
		e = nodecontrol.Privileged(ctx, os.Args[2], os.Stdin)
	case "rpc":
		r := bufio.NewReaderSize(os.Stdin, 4096)
		var line []byte
		line, e = r.ReadSlice('\n')
		if e != nil {
			os.Exit(1)
		}
		var c nodecontrol.Command
		if json.Unmarshal(line, &c) != nil {
			os.Exit(1)
		}
		if c.Action == "install" {
			cmd := exec.CommandContext(ctx, "sudo", "-n", "/usr/local/libexec/begom-node-helper", "privileged", "install")
			cmd.Stdin = io.MultiReader(strings.NewReader(string(line)), r)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			e = cmd.Run()
		} else {
			e = nodecontrol.Run(ctx, c, os.Stdout)
		}
	default:
		os.Exit(1)
	}
	if e != nil {
		io.WriteString(os.Stderr, "node operation failed\n")
		os.Exit(1)
	}
}
