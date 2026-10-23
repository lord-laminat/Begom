// Command openapi generates the wire schemas from the Go types and an explicit
// operation registry. CI compares this artifact and checks every server route.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/rychkin/Begom/internal/auth"
	"github.com/rychkin/Begom/internal/db"
	"github.com/rychkin/Begom/internal/model"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type obj = map[string]any

var schemas = obj{}

func ref(n string) obj { return obj{"$ref": "#/components/schemas/" + n} }
func str() obj         { return obj{"type": "string"} }
func array(s any) obj  { return obj{"type": "array", "items": s} }
func object(p obj) obj { return obj{"type": "object", "properties": p, "additionalProperties": false} }
func schema(t reflect.Type) any {
	if t == reflect.TypeFor[model.Number]() {
		return obj{"type": "string", "pattern": "^[0-9]{1,20}$", "description": "Unsigned decimal 0..18446744073709551615, without floating point conversion", "example": "18446744073709551615"}
	}
	if t == reflect.TypeFor[time.Time]() {
		return obj{"type": "string", "format": "date-time"}
	}
	if t.Kind() == reflect.Pointer {
		return obj{"anyOf": []any{schema(t.Elem()), obj{"type": "null"}}}
	}
	switch t.Kind() {
	case reflect.String:
		return str()
	case reflect.Bool:
		return obj{"type": "boolean"}
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		return obj{"type": "integer", "minimum": 0}
	case reflect.Float64:
		return obj{"type": "number"}
	case reflect.Slice, reflect.Array:
		return array(schema(t.Elem()))
	case reflect.Map:
		return obj{"type": "object", "additionalProperties": schema(t.Elem())}
	case reflect.Struct:
		if _, ok := schemas[t.Name()]; ok {
			return ref(t.Name())
		}
		schemas[t.Name()] = obj{}
		props := obj{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := f.Tag.Get("json")
			if tag == "-" || !f.IsExported() {
				continue
			}
			if f.Anonymous {
				parent := schema(f.Type).(obj)
				if r, ok := parent["$ref"].(string); ok {
					parent = schemas[strings.TrimPrefix(r, "#/components/schemas/")].(obj)
				}
				for k, v := range parent["properties"].(obj) {
					props[k] = v
				}
				continue
			}
			name := strings.Split(tag, ",")[0]
			if name == "" {
				name = f.Name
			}
			props[name] = schema(f.Type)
			if !strings.Contains(tag, "omitempty") {
				required = append(required, name)
			}
		}
		value := object(props)
		if t == reflect.TypeFor[model.WorkerConfig]() {
			value["required"] = []string{"worker_id", "threads"}
		} else if t != reflect.TypeFor[model.JobConfig]() {
			value["required"] = required
		} else {
			value["required"] = []string{"worker_ids"}
			value["description"] = "Defaults are applied; numeric range: range_text OR from/to. range_text accepts empty, N, [A,B], [A,]. max_items defines an immutable effective bound."
		}
		schemas[t.Name()] = value
		return ref(t.Name())
	default:
		return obj{}
	}
}
func main() {
	for _, v := range []any{model.Job{}, model.Worker{}, model.Progress{}, model.Permit{}, model.PermitRequest{}, model.Commit{}, model.Feedback{}, model.Heartbeat{}, model.HeartbeatReply{}, model.Lease{}, model.LeaseRef{}, model.Register{}, model.APIError{}, auth.User{}, auth.UserInput{}, db.Publication{}} {
		schema(reflect.TypeOf(v))
	}
	schemas["Empty"] = object(obj{})
	schemas["Login"] = object(obj{"username": str(), "password": obj{"type": "string", "writeOnly": true}})
	schemas["Session"] = object(obj{"user": ref("User"), "csrf_token": str()})
	schemas["JobCreated"] = object(obj{"job_id": str(), "run_id": str(), "config_version": obj{"type": "integer"}, "state": str(), "range": ref("Range"), "config": ref("JobConfig")})
	schemas["JobAction"] = object(obj{"job_id": str(), "state": str(), "run_id": str(), "reason": str()})
	schemas["ConfigReply"] = object(obj{"config_version": obj{"type": "integer"}, "config": ref("JobConfig")})
	schemas["Retry"] = object(obj{"external_ids": array(schema(reflect.TypeFor[model.Number]()))})
	schemas["Result"] = object(obj{"external_id": schema(reflect.TypeFor[model.Number]()), "status": str(), "error_code": str(), "updated_at": schema(reflect.TypeFor[time.Time]()), "metadata": ref("Metadata"), "file_id": str(), "size_bytes": schema(reflect.TypeFor[model.Number]()), "checksum": str(), "storage_path": str(), "downloaded_at": schema(reflect.TypeFor[time.Time]()), "worker_id": str()})
	schemas["Event"] = object(obj{"id": obj{"type": "integer"}, "timestamp": schema(reflect.TypeFor[time.Time]()), "component": str(), "worker_id": str(), "chunk_id": str(), "attempt_id": str(), "external_id": schema(reflect.TypeFor[model.Number]()), "operation": str(), "result": str(), "error_code": str()})
	schemas["ACL"] = object(obj{"user_id": str(), "permission": obj{"type": "string", "enum": []string{"view", "manage", "remove"}}})
	schemas["NodeAction"] = object(obj{"action": obj{"type": "string", "enum": []string{"check", "start", "stop", "restart", "install", "update"}}, "version": str()})
	schemas["Revoke"] = object(obj{"serial": str()})
	schemas["PermitReply"] = object(obj{"state": str(), "permit": ref("Permit"), "retry_after_ms": obj{"type": "integer"}})
	schemas["LeaseReply"] = object(obj{"lease": obj{"anyOf": []any{ref("Lease"), obj{"type": "null"}}}})
	schemas["ProgressReply"] = object(obj{"accepted": array(object(obj{"external_id": schema(reflect.TypeFor[model.Number]()), "status": str()})), "next_id": schema(reflect.TypeFor[*model.Number]())})
	schemas["ChunkReply"] = object(obj{"state": str(), "next_id": schema(reflect.TypeFor[*model.Number]())})
	schemas["Snapshot"] = obj{"type": "object", "properties": obj{"server_time": schema(reflect.TypeFor[time.Time]()), "jobs": array(ref("Job")), "workers": array(ref("Worker")), "source_profile": obj{"type": "object"}, "dispatcher": obj{"type": "object"}}}
	for name, field := range map[string]string{"Jobs": "jobs", "Workers": "workers", "Items": "items", "Events": "events", "Users": "users"} {
		typ := map[string]string{"Jobs": "Job", "Workers": "Worker", "Items": "Result", "Events": "Event", "Users": "User"}[name]
		schemas[name] = object(obj{field: array(ref(typ))})
	}
	schemas["Items"].(obj)["properties"].(obj)["complete"] = obj{"type": "boolean"}
	schemas["Items"].(obj)["properties"].(obj)["count"] = obj{"type": "integer"}
	schemas["Jobs"].(obj)["properties"].(obj)["next_before"] = str()
	schemas["Acknowledgement"] = obj{"type": "object", "description": "Operation-specific confirmation; see contracts.md", "additionalProperties": true}
	// method | path | request schema | response schema | status | access | behavior
	registry := `
POST|/api/auth/login|Login|Session|200|public|Create an eight-hour secure session; ten attempts per IP per minute
GET|/api/auth/session||Session|200|session|Current identity and CSRF token
POST|/api/auth/logout|Empty|Acknowledgement|200|session|Revoke current session
GET|/api/jobs||Jobs|200|session|Jobs filtered by ACL, newest first; before job ID cursor and limit 1..200
POST|/api/jobs|JobConfig|JobCreated|201|operator|Create lazily split numeric range
GET|/api/jobs/{jobId}||Job|200|view|Status and continuous confirmed progress
POST|/api/jobs/{jobId}/start|Empty|JobAction|200|manage|Validate resources and start
POST|/api/jobs/{jobId}/pause|Empty|JobAction|200|manage|Finish active items then pause
POST|/api/jobs/{jobId}/resume|Empty|JobAction|200|manage|Resume with a new run identifier
POST|/api/jobs/{jobId}/stop|Empty|JobAction|200|manage|Invalidate active attempts and stop
PATCH|/api/jobs/{jobId}/config|JobConfig|ConfigReply|200|manage|Replace hot parameters; original effective range remains fixed
POST|/api/jobs/{jobId}/items/retry|Retry|Acknowledgement|200|manage|Reopen selected failed items; completed jobs require resume
GET|/api/jobs/{jobId}/items||Items|200|view|Results ordered by external_id, status/after/limit filters
GET|/api/jobs/{jobId}/events||Events|200|view|Events with worker_id/status/from/to/after/limit filters
GET|/api/jobs/{jobId}/export||Items|200|view|Stream all filtered results as CSV or JSON; format required, after optional
GET|/api/jobs/{jobId}/access||Acknowledgement|200|manage|List assignment ACL
PUT|/api/jobs/{jobId}/access|ACL|Acknowledgement|200|manage|Grant or remove access; owner cannot be removed
GET|/api/workers||Workers|200|session|Available worker pool, metrics and active work filtered by role and ACL
POST|/api/workers|WorkerConfig|Worker|200|admin|Add a worker; credentials are secret references
GET|/api/workers/{workerId}||Worker|200|session|Worker state with ACL filtering
PATCH|/api/workers/{workerId}|WorkerConfig|Worker|200|admin|Replace global worker configuration
GET|/api/workers/{workerId}/metrics||Acknowledgement|200|session|Latest worker resources, filtered by assignment ACL
GET|/api/workers/{workerId}/actions||Acknowledgement|200|admin|SSH action outcomes, including outcome_unknown
POST|/api/workers/{workerId}/actions|NodeAction|Acknowledgement|202|admin|Durable node-management intent and asynchronous verified SSH
POST|/api/workers/{workerId}/certificates/revoke|Revoke|Acknowledgement|200|admin|Revoke serial, including existing control connections
POST|/api/workers/{workerId}/certificates/rotate|Empty|Acknowledgement|200|admin|Issue next short-lived identity in OpenBao
GET|/api/users||Users|200|admin|List local accounts
POST|/api/users|UserInput|User|201|admin|Create account using Argon2id
PATCH|/api/users/{userId}|UserInput|User|200|admin|Update account and invalidate sessions
GET|/api/audit||Acknowledgement|200|admin|Administrative audit log
GET|/api/snapshot||Snapshot|200|session|Complete current ACL-filtered interface snapshot
GET|/api/events/stream||Snapshot|101|session|WebSocket snapshot every second; reread HTTP on reconnect
POST|/api/workers/{workerId}/ssh/sessions|Empty|Acknowledgement|201|admin|One-use ticket, thirty seconds; transmit in first WSS frame
GET|/api/workers/{workerId}/ssh/stream||Acknowledgement|101|admin|Restricted terminal, fifteen minutes total and two minutes idle
GET|/api/files/{fileId}/content|||200|view|Published catalog entry only; authenticated NGINX internal redirect; Range and If-Range
POST|/api/workers/register|Register|Acknowledgement|200|mtls|Bind worker certificate to a compatible agent session
POST|/api/workers/{workerId}/heartbeat|Heartbeat|HeartbeatReply|200|mtls|Renew matching leases and submit one-second measurements
POST|/api/workers/{workerId}/lease|Empty|LeaseReply|200|mtls|Transactionally acquire at most one active block per worker
POST|/api/workers/{workerId}/progress|Progress|ProgressReply|200|mtls|Commit item results; sequence cannot regress state and gaps remain visible
POST|/api/workers/{workerId}/complete|LeaseRef|ChunkReply|200|mtls|Complete a block only after every identifier has a final result
POST|/api/workers/{workerId}/fail|LeaseRef|ChunkReply|200|mtls|Release an incomplete block for recovery
POST|/api/workers/{workerId}/permits|PermitRequest|PermitReply|202|mtls|Enqueue one actual HTTP request; poll permit by UID
GET|/api/workers/{workerId}/permits/{uid}||PermitReply|200|mtls|Read generation-bound permit; never reuse a consumed permit
DELETE|/api/workers/{workerId}/permits/{uid}|Empty|Acknowledgement|200|mtls|Cancel pending demand
POST|/api/workers/{workerId}/connections/{connectionId}/renew|Empty|Acknowledgement|200|mtls|Renew an existing connection slot; no new HTTP request is authorized
POST|/api/workers/{workerId}/connections/{connectionId}/release|Feedback|Acknowledgement|200|mtls|Release connection and persist Retry-After and adaptive load state
POST|/api/workers/{workerId}/downloads/{downloadId}/commit|Commit|Acknowledgement|202|mtls|Start verified staged publication
GET|/api/workers/{workerId}/publications/{publicationId}||Publication|200|mtls|Publication confirmation or diagnosed failure
GET|/api/workers/{workerId}/control||Acknowledgement|101|mtls|Control WebSocket every 250 ms; cancellation on epoch or authority loss
GET|/metrics|||200|metrics|Prometheus text, restricted service identity
GET|/healthz|||200|public|Liveness, does not imply dependencies are ready
GET|/readyz|||200|public|SQL, mounted volume and dispatcher generation readiness
GET|/internal/grafana-auth|||204|admin|Internal NGINX administrator authentication subrequest
`
	paths := obj{}
	pattern := regexp.MustCompile(`\{([^}]+)\}`)
	for _, line := range strings.Split(strings.TrimSpace(registry), "\n") {
		f := strings.Split(line, "|")
		method, path, request, response, status, access, description := f[0], f[1], f[2], f[3], f[4], f[5], f[6]
		params := []any{obj{"name": "API-Version", "in": "header", "schema": obj{"type": "string", "enum": []string{"1"}}, "description": "Omitted means v1; unsupported version returns 426"}}
		for _, m := range pattern.FindAllStringSubmatch(path, -1) {
			params = append(params, obj{"name": m[1], "in": "path", "required": true, "schema": str()})
		}
		mut := method != "GET"
		if path == "/api/jobs" && !mut {
			for _, name := range []string{"before", "limit"} {
				params = append(params, obj{"name": name, "in": "query", "schema": str()})
			}
		}
		if strings.HasSuffix(path, "/items") || strings.HasSuffix(path, "/events") || strings.HasSuffix(path, "/export") {
			for _, name := range []string{"status", "after", "limit", "from", "to", "worker_id", "format"} {
				params = append(params, obj{"name": name, "in": "query", "schema": str(), "description": "Applicable filters and bounds are specified in contracts.md"})
			}
		}
		if mut && path != "/api/auth/login" && path != "/api/auth/logout" {
			params = append(params, obj{"name": "Idempotency-Key", "in": "header", "required": true, "schema": obj{"type": "string", "minLength": 1, "maxLength": 200}, "description": "Same actor, method, path and exact body hash replay the confirmed response; changed body returns 409"})
		}
		if mut && access != "mtls" && access != "public" {
			params = append(params, obj{"name": "X-CSRF-Token", "in": "header", "required": true, "schema": str()})
		}
		replies := obj{}
		success := obj{"description": description}
		if response != "" {
			success["content"] = obj{"application/json": obj{"schema": ref(response)}}
		}
		if strings.HasSuffix(path, "/export") {
			success["content"].(obj)["text/csv"] = obj{"schema": str()}
		}
		replies[status] = success
		for _, code := range []string{"400", "401", "403", "404", "409", "413", "422", "426", "429", "503"} {
			replies[code] = obj{"description": "Structured error; see error codes and retry rules in contracts.md", "content": obj{"application/json": obj{"schema": ref("APIError")}}}
		}
		op := obj{"summary": description, "operationId": strings.ToLower(method) + "_" + pattern.ReplaceAllString(strings.ReplaceAll(strings.Trim(path, "/"), "/", "_"), "$1"), "parameters": params, "responses": replies, "x-required-access": access}
		if request != "" {
			op["requestBody"] = obj{"required": true, "content": obj{"application/json": obj{"schema": ref(request)}}}
		}
		switch access {
		case "public":
			op["security"] = []any{}
		case "mtls":
			if path != "/api/workers/register" {
				op["parameters"] = append(params, obj{"name": "Agent-Session", "in": "header", "required": true, "schema": str(), "description": "Current SQL-confirmed registration session, also required for WSS control"})
			}
			op["security"] = []any{obj{"agentCertificate": []string{}}}
			op["servers"] = []any{obj{"url": "https://coordinator:8444"}}
		case "metrics":
			op["security"] = []any{obj{"metricsCertificate": []string{}}}
		default:
			op["security"] = []any{obj{"sessionCookie": []string{}}}
		}
		if _, ok := paths[path]; !ok {
			paths[path] = obj{}
		}
		paths[path].(obj)[strings.ToLower(method)] = op
	}
	doc := obj{"openapi": "3.1.0", "info": obj{"title": "Begom API", "version": model.Version, "description": "Version 1. Numeric identifiers are decimal strings. See docs/contracts.md for lifecycle, pagination, WSS frames and safety invariants."}, "servers": []any{obj{"url": "https://localhost:8443"}}, "paths": paths, "components": obj{"schemas": schemas, "securitySchemes": obj{"sessionCookie": obj{"type": "apiKey", "in": "cookie", "name": auth.SessionCookie}, "agentCertificate": obj{"type": "mutualTLS", "description": "CA verified, worker URI bound to route, serial not revoked"}, "metricsCertificate": obj{"type": "mutualTLS", "description": "spiffe://begom/service/prometheus"}}}}
	b, e := json.MarshalIndent(doc, "", "  ")
	if e != nil {
		panic(e)
	}
	if e = os.WriteFile("docs/openapi.json", append(b, '\n'), 0644); e != nil {
		panic(e)
	}
	fmt.Println("docs/openapi.json generated")
}
