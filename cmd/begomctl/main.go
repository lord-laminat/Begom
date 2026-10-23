package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/rychkin/Begom/internal/bootstrap"
	"github.com/rychkin/Begom/internal/config"
	"github.com/rychkin/Begom/internal/model"
	"github.com/rychkin/Begom/internal/wire"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, "begomctl:", e)
		os.Exit(1)
	}
}
func printJSON(v any) error {
	e := json.NewEncoder(os.Stdout)
	e.SetIndent("", "  ")
	return e.Encode(v)
}
func flags(name string, args []string, setup func(*flag.FlagSet)) error {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	setup(f)
	return f.Parse(args)
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("commands: init, bao-init, permissions, health, jobs, workers, package, rotate-worker, acceptance, backup, restore")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	switch args[0] {
	case "renew-leaf":
		dir, name := ".local", "web"
		if e := flags(args[0], args[1:], func(f *flag.FlagSet) {
			f.StringVar(&dir, "dir", dir, "profile directory")
			f.StringVar(&name, "name", name, "certificate name")
		}); e != nil {
			return e
		}
		return bootstrap.RenewLeaf(dir, name)
	case "linux-init":
		file, dir := "deploy/linux-profile.example.json", ".local-linux"
		if e := flags(args[0], args[1:], func(f *flag.FlagSet) {
			f.StringVar(&file, "file", file, "DNS profile JSON")
			f.StringVar(&dir, "dir", dir, "new private output directory")
		}); e != nil {
			return e
		}
		var p bootstrap.LinuxProfile
		if e := config.Read(file, &p); e != nil {
			return errors.New("invalid Linux profile")
		}
		return bootstrap.InitLinux(dir, p)
	case "init", "permissions", "bao-init", "rotate-worker":
		dir, endpoint, worker, source := ".local", "https://127.0.0.1:8200", "", "mock"
		e := flags(args[0], args[1:], func(f *flag.FlagSet) {
			f.StringVar(&dir, "dir", dir, "generated profile directory")
			f.StringVar(&endpoint, "url", endpoint, "OpenBao URL")
			f.StringVar(&worker, "worker", worker, "worker identity")
			f.StringVar(&source, "source", source, "mock or kinescope (init only)")
		})
		if e != nil {
			return e
		}
		switch args[0] {
		case "init":
			if e = bootstrap.InitSource(dir, source); e == nil {
				fmt.Println("Локальный профиль создан. Пароль находится в " + dir + "/client-password; секреты не выводятся.")
			}
		case "permissions":
			e = bootstrap.Permissions(dir)
		case "bao-init":
			e = bootstrap.BaoInit(ctx, dir, endpoint)
		case "rotate-worker":
			e = bootstrap.RotateWorker(ctx, dir, endpoint, worker)
		}
		return e
	case "package":
		agent, key, out, version := "bin/agent", ".local/bootstrap/package.key", ".local/packages", model.Version
		e := flags("package", args[1:], func(f *flag.FlagSet) {
			f.StringVar(&agent, "agent", agent, "agent binary")
			f.StringVar(&key, "key", key, "signing key file")
			f.StringVar(&out, "output", out, "output directory")
			f.StringVar(&version, "version", version, "release version")
		})
		if e != nil {
			return e
		}
		return bootstrap.Package(agent, key, out, version)
	case "health":
		return health(args[1:])
	case "jobs", "workers":
		return clientCommand(ctx, args)
	default:
		return extraCommand(ctx, args)
	}
}
func login(ctx context.Context, path string) (*wire.Client, config.Client, error) {
	var cfg config.Client
	if e := config.Read(path, &cfg); e != nil {
		return nil, cfg, e
	}
	c, e := wire.New(cfg.URL, config.TLS{CA: cfg.CA}, false)
	if e != nil {
		return nil, cfg, e
	}
	password, e := config.Secret(cfg.PasswordFile)
	if e != nil {
		return nil, cfg, e
	}
	var reply struct {
		CSRF string `json:"csrf_token"`
	}
	if e = c.Do(ctx, "POST", "/api/auth/login", map[string]string{"username": cfg.Username, "password": password}, &reply, ""); e != nil {
		return nil, cfg, e
	}
	c.CSRF = reply.CSRF
	return c, cfg, nil
}
func clientCommand(ctx context.Context, args []string) error {
	if len(args) < 2 {
		return errors.New("supply an action")
	}
	group, action := args[0], args[1]
	file, id, input, rangeText, from, to, workers, nodeAction, version := ".local/client.json", "", "", "", "", "", "", "check", model.Version
	threads, block, retries := 8, uint64(1000), 4
	rps := 0.0
	rangeSet := false
	f := flag.NewFlagSet(group+" "+action, flag.ContinueOnError)
	f.StringVar(&file, "config", file, "client profile")
	f.StringVar(&id, "id", id, "job or worker ID")
	f.StringVar(&input, "file", input, "request JSON file")
	f.Func("range", "range expression", func(s string) error { rangeText = s; rangeSet = true; return nil })
	f.StringVar(&from, "from", from, "first numeric ID")
	f.StringVar(&to, "to", to, "last numeric ID")
	f.StringVar(&workers, "workers", workers, "comma-separated worker IDs")
	f.IntVar(&threads, "threads", threads, "default threads")
	f.Uint64Var(&block, "block-size", block, "IDs per block")
	f.IntVar(&retries, "retries", retries, "retry limit")
	f.Float64Var(&rps, "rps", rps, "job rate limit")
	f.StringVar(&nodeAction, "action", nodeAction, "node action")
	f.StringVar(&version, "version", version, "signed package version")
	if e := f.Parse(args[2:]); e != nil {
		return e
	}
	c, _, e := login(ctx, file)
	if e != nil {
		return e
	}
	method, p := "GET", "/api/"+group
	var body any
	if group == "jobs" {
		switch action {
		case "list":
		case "show", "items", "events":
			if !model.ValidID(id) {
				return errors.New("valid --id required")
			}
			p += "/" + id
			if action != "show" {
				p += "/" + action
			}
		case "create":
			var cfg model.JobConfig
			if input != "" {
				if e = config.Read(input, &cfg); e != nil {
					return e
				}
			} else {
				cfg = model.JobConfig{WorkerIDs: strings.Split(workers, ","), Threads: threads, BlockSize: block, Retry: model.RetryPolicy{MaxRetries: retries}, Limits: model.Limits{RPS: rps}}
				if workers == "" {
					return errors.New("--workers is required")
				}
				if rangeSet {
					cfg.RangeText = &rangeText
				}
				if from != "" {
					var n model.Number
					if e = n.UnmarshalJSON([]byte(from)); e != nil {
						return e
					}
					cfg.From = &n
				}
				if to != "" {
					var n model.Number
					if e = n.UnmarshalJSON([]byte(to)); e != nil {
						return e
					}
					cfg.To = &n
				}
			}
			if _, e = cfg.Normalize(); e != nil {
				return e
			}
			method = "POST"
			body = cfg
		case "start", "pause", "resume", "stop":
			if !model.ValidID(id) {
				return errors.New("valid --id required")
			}
			method = "POST"
			p += "/" + id + "/" + action
			body = struct{}{}
		case "configure":
			if !model.ValidID(id) || input == "" {
				return errors.New("--id and --file required")
			}
			var cfg model.JobConfig
			if e = config.Read(input, &cfg); e != nil {
				return e
			}
			method = "PATCH"
			p += "/" + id + "/config"
			body = cfg
		default:
			return errors.New("unknown job action")
		}
	} else {
		switch action {
		case "list":
		case "show", "metrics":
			if !model.ValidID(id) {
				return errors.New("valid --id required")
			}
			p += "/" + id
			if action == "metrics" {
				p += "/metrics"
			}
		case "action":
			if !model.ValidID(id) {
				return errors.New("valid --id required")
			}
			method = "POST"
			p += "/" + id + "/actions"
			body = map[string]string{"action": nodeAction, "version": version}
		case "configure", "create":
			var cfg model.WorkerConfig
			if input == "" {
				return errors.New("--file required")
			}
			if e = config.Read(input, &cfg); e != nil {
				return e
			}
			method = "POST"
			if action == "configure" {
				method = "PATCH"
				p += "/" + cfg.ID
			}
			body = cfg
		default:
			return errors.New("unknown worker action")
		}
	}
	var result any
	key := ""
	if method != "GET" {
		key = model.NewID()
	}
	if e = c.Retry(ctx, method, p, body, &result, key); e != nil {
		return e
	}
	return printJSON(result)
}
func health(args []string) error {
	raw, ca, cert, key := "http://127.0.0.1:8080/readyz", "", "", ""
	if e := flags("health", args, func(f *flag.FlagSet) {
		f.StringVar(&raw, "url", raw, "health URL")
		f.StringVar(&ca, "ca", ca, "CA file")
		f.StringVar(&cert, "cert", cert, "client certificate")
		f.StringVar(&key, "key", key, "client key")
	}); e != nil {
		return e
	}
	u, e := url.Parse(raw)
	if e != nil {
		return e
	}
	var tc *tls.Config
	if u.Scheme == "https" {
		tc, e = wire.TLS(config.TLS{CA: ca, Cert: cert, Key: key}, cert != "")
		if e != nil {
			return e
		}
	} else if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" {
		return errors.New("plain HTTP health checks are local only")
	}
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: tc, Proxy: nil}, Timeout: 3 * time.Second}
	r, e := c.Get(raw)
	if e != nil {
		return errors.New("health endpoint unavailable")
	}
	defer r.Body.Close()
	io.Copy(io.Discard, io.LimitReader(r.Body, 4096))
	if r.StatusCode != 200 {
		return errors.New("service is not ready")
	}
	return nil
}
