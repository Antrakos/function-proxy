# function-proxy

A Crossplane composition function that proxies `RunFunctionRequest` calls to a
backend gRPC endpoint, substituting the step's input with an arbitrary payload
the backend expects.

## Why

Crossplane names a composition function's runtime `Deployment` after its
`FunctionRevision`. Every `spec.package` digest change creates a new revision,
which deletes the old Deployment and creates a new one (not a rolling update).
During that gap the function's gRPC endpoint is unavailable, causing transient
`Unhealthy` flaps on every deploy ([crossplane/crossplane#7298]).

`function-proxy` solves this by being a **pinned, never-republished** function
package — no revision churn, no Deployment recreation, no XR flaps. Your real
logic lives in a self-managed `Deployment` + `Service` that you roll out with
ordinary zero-downtime strategies.

## Usage

Reference `function-proxy` in your composition pipeline and point it at your
backend:

```yaml
apiVersion: apiextensions.crossplane.io/v1
kind: Composition
spec:
  compositeTypeRef:
    apiVersion: example.crossplane.io/v1
    kind: XR
  mode: Pipeline
  pipeline:
    - step: forward-to-backend
      functionRef:
        name: function-proxy
      input:
        apiVersion: proxy.fn.antrakos.github.io/v1beta1
        kind: ProxyInput
        backend:
          url: function-backend.my-namespace:9443
          # timeout: 0s  # 0 = inherit Crossplane's function timeout
        payload:
          apiVersion: template.fn.crossplane.io/v1beta1
          kind: Input
          # ... whatever the backend function expects
```

The `service.namespace:port` shorthand in `backend.url` is expanded to
`dns:///service.namespace.svc.cluster.local:port` automatically. You can also
use a fully qualified target or an explicit gRPC resolver scheme
(e.g. `dns:///host:port`) — see the `ProxyInput` CRD for details.

## Backend requirements

The backend is any gRPC server that implements the Crossplane
`FunctionRunnerService` proto — the same interface a normal composition
function exposes. However, it does **not** need to be deployed as a Crossplane
`Function` CRD. A plain `Deployment` + `Service` is sufficient.

**The backend must accept plaintext gRPC (h2c).** In Crossplane terms, start it
with `--insecure`. The proxy always dials the backend over insecure h2c;
transport security is expected to be provided by a service mesh (e.g. Linkerd,
Istio) handling mTLS on the wire.

**Expose the backend with a headless Service** (`clusterIP: None`) when running
more than one replica. The proxy dials with gRPC `round_robin` load balancing
so each `RunFunctionRequest` is spread across all backend pods. A headless
Service makes DNS return every pod IP, which is what `round_robin` needs; a
regular ClusterIP Service exposes a single virtual IP, so gRPC would pin its one
long-lived HTTP/2 connection to a single pod and leave the other replicas idle.
This mirrors how Crossplane itself dials normally packaged functions. The proxy
also dials with `waitForReady`, so calls wait for a ready backend (e.g. during a
rollout) rather than failing fast, bounded by the call's timeout.

## Key benefits

- **Zero XR flaps on backend deploys** — pinned proxy image means no
  `FunctionRevision` churn; backend rolls out with normal Kubernetes rolling
  updates.
- **Decoupled release cadence** — update the backend image as often as you like
  without touching Crossplane's package machinery.
- **Per-step routing** — repoint `backend.url` (e.g. v1 → v2 canary Service)
  by editing the composition only; no function reinstall, no pod restart.
- **Forward compatible** — unknown protobuf fields pass through byte-faithfully,
  so the proxy never needs an update just to forward new Crossplane gRPC fields.
- **Minimal footprint** — stateless proxy with connection pooling; adds < ~5 ms
  p99 latency per call in-cluster.
