# ADR 0001: `function-proxy` architecture and key decisions

- Status: Accepted
- Date: 2026-06-02

## Context

Crossplane names a composition function's runtime `Deployment` after its
`FunctionRevision`. Every `spec.package` digest change creates a new revision →
the old runtime Deployment is deleted and a new one created (not a rolling
update). During the gap the function's gRPC endpoint is unavailable and in-flight
reconciles fail, producing transient XR `Unhealthy` flaps on every deploy
(crossplane/crossplane#7298).

We want the release cadence of our real composition logic to be **independent**
of Crossplane's package machinery, with normal Kubernetes rollout semantics,
while keeping a stable, Crossplane-blessed entry point that compositions
reference.

The solution is `function-proxy`: a tiny, stateless Crossplane function whose
image/logic never changes (pinned package digest → no revision churn). It
forwards each `RunFunctionRequest` to a backend gRPC endpoint chosen per
composition step from its own `input`, after substituting the `input` payload the
backend should receive, and returns the backend's response verbatim. The real
logic lives in a self-managed Deployment + Service rolled with ordinary
zero-downtime strategies.

### Non-goals

- **Not** a general-purpose API gateway, retry / rate-limiter, or auth broker.
- **Not** responsible for the backend's rollout, scaling, or health — that is the
  self-managed Deployment's concern.
- **Does not interpret or mutate composition semantics** (observed/desired
  resources). It only swaps the `input` field and rewrites the destination. This
  is the core invariant: every `RunFunctionRequest` field except `input`, and the
  entire `RunFunctionResponse`, pass through byte-faithful.

## Decision

Implement the proxy as a normal `RunFunction` handler that **decodes each
request, substitutes its `input`, and forwards it** to the backend — built on
the `function-sdk-go` `function.Serve` bootstrap, with **insecure (h2c)
outbound** to the backend.

### Architecture

```
            Crossplane function-runner
                     │ mTLS :9443  (certs @ /tls/server, injected)
                     ▼
        ┌─────────────────────────────────────┐
        │ function-proxy (PINNED xpkg)         │
        │  RunFunction(req):                   │
        │    in   = parse(req.input)           │
        │    req' = req with input=in.payload  │
        │    conn = pool.get(in.backend)       │
        │    resp = client(conn).Run(req')     │
        │    return resp  // verbatim          │
        └───────────────┬─────────────────────┘
                         │ gRPC h2c (insecure), target from input.backend
                         ▼
        ┌─────────────────────────────────────┐
        │ real function — SELF-MANAGED          │
        │ normal Deployment + Service/Ingress   │
        │ rolling updates, HPA, own CI/CD       │
        └─────────────────────────────────────┘
```

### Key decisions

1. **Decode + substitute, not transparent byte passthrough.** The proxy must
   replace the `input` it forwards, which requires decoding the protobuf message,
   so we implement `RunFunction` directly. A transparent raw-codec proxy (a
   `StreamDirector` à la `mwitkow/grpc-proxy` that streams bytes through without
   decoding) cannot substitute `input`, and would also require owning
   `grpc.NewServer` with an unknown-service handler and raw codec — incompatible
   with `function.Serve`. Its only upside is latency, and the cost of
   decode/re-encoding a tiny `input` field is negligible against the network hop.
   Recorded as a possible future optimization for steps that need no
   substitution.

2. **Build on `function.Serve`.** It provides, for free: inbound mTLS from the
   Crossplane-injected certs (`tls.crt`/`tls.key`/`ca.crt` via
   `TLS_SERVER_CERTS_DIR`, default `/tls/server`), dual v1/v1beta1 service
   registration, a Prometheus metrics server on `:8080` with gRPC server-side
   metrics, and a `WithHealthServer` hook. Keeps us aligned with the "tiny,
   minimal deps" goal. We accept its constraints (see Consequences re: response
   size) and revisit only if forced.

3. **Outbound = insecure (h2c) only; no configurable backend TLS for now.** The
   proxy always dials the backend plaintext h2c (`insecure.NewCredentials()`).
   The realistic production path is a self-managed backend behind a service mesh
   that provides mTLS, so the mesh handles transport security on the wire. This
   means the **backend must be started with `--insecure`** (a default
   mTLS-configured function would reject a plaintext dial at the TLS handshake) —
   a contract on the backend's Deployment, to be documented in the README.
   `backend.tls` is intentionally **not** part of `ProxyInput` yet; a
   re-origination mTLS path (reuse Crossplane-injected certs, requires the backend
   to trust the same CA) can be added later if a Crossplane-CA-trusting backend
   appears.

4. **Typed `ProxyInput` with `payload` as `RawExtension`.** Define the input via
   the kubebuilder-annotated struct so `controller-tools` generates the input CRD
   and Crossplane validates step input. `backend` is a typed struct; `payload` is
   arbitrary KRM, so it is held as `runtime.RawExtension` and re-marshaled into a
   `structpb.Struct` placed into `req.Input` for the backend. The proxy treats
   `payload` as opaque. Preferred over hand-walking raw `structpb` because the
   marshaling cost is trivial relative to the (large) request and we keep
   CRD-level validation. The input group/version becomes
   `proxy.fn.antrakos.github.io/v1alpha1` (to be set on the struct's
   `+groupName`/`+versionName` markers; CRD regenerated).

5. **Connection pooling: simple keyed map for MVP.** Cache `*grpc.ClientConn`
   keyed by *resolved target* (`backend.url`). Lazy dial on first use; gRPC
   `ClientConn` is goroutine-safe and
   self-reconnecting, so a never-evicting map is robust for a small, stable set
   of backends. Upgrade to an LRU with idle eviction only when the
   backend set becomes unbounded (e.g. frequent ephemeral canary Services). Never
   dial per call — that would blow the <5 ms p99 budget.

6. **Message sizes.** Set generous client call options
   (`grpc.MaxCallRecvMsgSize`/`grpc.MaxCallSendMsgSize`, target 256 MB) on the
   outbound leg, and raise the inbound `MaxRecvMessageSize` (currently defaulted
   to 4 MB in `main.go`). See Open Risk below re: the inbound *send* (response)
   size limit, which `function.Serve` does not currently expose.

7. **Client-side load balancing: `round_robin` + headless backend Service.** Dial
   the backend with the gRPC service config
   `{"loadBalancingConfig":[{"round_robin":{}}],"methodConfig":[{"name":[{}],"waitForReady":true}]}`.
   This is the same configuration Crossplane's own function-runner applies when
   it calls a packaged function, and we adopt it for the same reason: a function
   backend is expected to run multiple replicas, and gRPC's default `pick_first`
   policy would pin a single long-lived HTTP/2 connection to one pod and route
   every request there, leaving the other replicas idle. `round_robin` spreads
   each call across all backend pods, but only when the resolver returns multiple
   addresses — so the **backend Service must be headless (`clusterIP: None`)**, a
   contract documented in the README alongside `--insecure`. A ClusterIP Service
   exposes one virtual IP and defeats client-side balancing. Our
   `service.namespace:port` shorthand already resolves to a `dns:///` target (see
   Backend resolution), which the dns resolver re-resolves as pods churn during a
   rollout. `waitForReady` additionally makes calls wait for a ready backend
   (e.g. mid-rollout) instead of failing fast, bounded by the call's own
   timeout/deadline — this slightly changes failure timing relative to a bare
   dial (a not-yet-ready backend yields a deadline rather than an immediate
   connection error), which is the desired behavior for rolling backends.

### Backend resolution (`ProxyInput.backend`)

- `url`: the gRPC target. It recognises exactly one shorthand and otherwise
  stays out of the way, applying these rules in order:
  - Any value containing `://` (an explicit gRPC resolver scheme, e.g.
    `dns:///host:port`, `unix:///path`) is used **verbatim**. This is the escape
    hatch for targets the shorthand would otherwise rewrite — notably an external
    two-label domain such as `dns:///example.com:9443`.
  - A `host:port` whose host is already fully qualified (ends in
    `.svc.cluster.local`, ends in `.`, or is an IP literal) is used **verbatim**.
  - A two-label `service.namespace:port` is **expanded** to
    `dns:///service.namespace.svc.cluster.local:port`, following Kubernetes DNS
    order (first label = Service, second = namespace). This is the convenience
    shorthand for the common in-cluster case.
  - Anything else (single label, three+ labels, no port, unparseable) is left
    **verbatim** for gRPC to resolve or reject. A bare single-label
    `service:port` is intentionally *not* expanded: that would require the proxy
    to know its own namespace, which we keep it free of.
  The pool is keyed by the *resolved* target, so the shorthand and its expanded
  FQDN share one connection. Transport security is not encoded in the URL scheme:
  the proxy always dials insecure h2c (see decision 3), there is no `tls` field.
- `timeout`: `0s` inherits Crossplane's function timeout.

### `ProxyInput` schema (step input)

```yaml
apiVersion: proxy.fn.antrakos.github.io/v1alpha1
kind: ProxyInput
# --- routing ---
backend:                        # required
  url: function-backend.example-system:9443
  # "service.namespace:port" shorthand → dns:///…svc.cluster.local:port.
  # A full host:port or an explicit dns:/// target is also accepted (verbatim).
  # Always dialed insecure h2c; the backend must run with --insecure (mesh mTLS).
  timeout: 0s                   # 0 = inherit Crossplane's function timeout
# --- payload forwarded to the backend as ITS input (opaque to the proxy) ---
payload:                        # required
  apiVersion: template.fn.crossplane.io/v1beta1
  kind: Input
  composite: <whatever the backend function expects>
```

### Failure behavior

- Backend dial/timeout/error → propagate gRPC status, or return a
  `RunFunctionResponse` with a `FATAL` result, so the reconcile retries rather
  than silently dropping resources.
- Malformed `input` (missing `backend`/`payload`) → `FATAL` with a clear message;
  never forward an empty input.
- Never log payloads at info level (may contain `credentials`); payload logging
  is debug-only and redacted.

## Consequences

### Positive

- Pinned, immutable proxy image → no `FunctionRevision` churn → stable runtime
  Deployment/Service → zero XR flaps on backend deploys.
- Backend release cadence fully decoupled from Crossplane packaging; normal k8s
  rolling updates, HPA, PDB, own CI/CD.
- Per-step backend routing via `input` — repoint v1→v2/canary by editing the
  composition only, no function reinstall, no pod restart.
- Minimal code and deps: the gRPC client stub
  (`v1.NewFunctionRunnerServiceClient`) ships in `function-sdk-go`, so forwarding
  needs no dependency beyond the already-transitive `google.golang.org/grpc`.

### Negative / accepted trade-offs

- Adds one network hop and a protobuf decode/re-encode of `input` per call
  (target added p99 < ~5 ms in-cluster).
- Backend availability is now the self-managed Deployment's responsibility
  (mitigate with PDB + `maxUnavailable: 0` + readiness gates).
- The proxy is still technically a Crossplane Function; bumping its image would
  reintroduce churn. Treat the package as immutable — no image automation; bump
  only for CVEs.
- The backend's transport security depends entirely on the mesh / trusted
  network: the proxy always dials insecure h2c, and the backend must run with
  `--insecure`. Configurable backend TLS (incl. re-origination mTLS) is deferred.

### Open risks to validate during implementation

- **256 MB response path:** `function.Serve` sets the server's receive size but
  does not expose `grpc.MaxSendMsgSize`, so responses back to Crossplane may be
  capped at the gRPC default (~4 MB). This is the load-bearing risk. Spike it
  first: if `function.Serve` cannot carry large responses, we must either build
  `grpc.NewServer` ourselves (revisiting decision 2) or upstream a
  `MaxSendMessageSize` serve option to `function-sdk-go`.
- **v1 vs v1beta1 backend:** the proxy calls the backend over v1. A backend that
  only speaks v1beta1 is an edge case to handle if it arises.

## Acceptance criteria

- Deploying a new backend image triggers a **rolling update only**: no
  `FunctionRevision` created, no runtime Deployment recreated, **zero** XR
  `Unhealthy` flaps observed during the deploy.
- `crossplane render` of a composition through `function-proxy` produces output
  **identical** to calling the backend directly with `payload` as its input.
- Switching `backend` in `input` (e.g. v1→v2 Service) reroutes with **no**
  function reinstall and **no** pod restart.
- Inbound mTLS is verified; malformed input yields a clear `FATAL`; a backend
  outage yields a retryable error (not silent resource loss).
- Added p99 latency over a direct call is **< ~5 ms** in-cluster.

