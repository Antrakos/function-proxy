package main

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Antrakos/function-proxy/input/v1beta1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/crossplane/function-sdk-go/errors"
	"github.com/crossplane/function-sdk-go/logging"
	fnv1 "github.com/crossplane/function-sdk-go/proto/v1"
	"github.com/crossplane/function-sdk-go/request"
	"github.com/crossplane/function-sdk-go/response"
)

const (
	// maxMsgSizeBytes is the maximum gRPC message size for backend calls (256 MB).
	maxMsgSizeBytes = 256 * 1024 * 1024

	// clusterDomainSuffix is appended to the two-label "service.namespace"
	// shorthand to form a fully qualified in-cluster Service DNS name.
	clusterDomainSuffix = "svc.cluster.local"

	// backendServiceConfig is the gRPC service config applied to every backend
	// connection. It mirrors how Crossplane's own function-runner dials a
	// function so a multi-replica backend behaves identically to a normally
	// packaged function:
	//
	//   - round_robin load balancing spreads each RPC across every backend pod.
	//     With the default pick_first policy gRPC would pin one long-lived HTTP/2
	//     connection to a single pod and send all traffic there, leaving the
	//     other replicas idle. round_robin only takes effect when the dns
	//     resolver returns multiple addresses, so the backend Service must be
	//     headless (clusterIP: None) — a ClusterIP exposes one virtual IP and
	//     defeats client-side balancing. The "service.namespace:port" shorthand
	//     already resolves to a dns:/// target, which the resolver re-resolves as
	//     pods come and go during a rollout.
	//   - waitForReady makes RPCs wait for a ready backend instead of failing
	//     fast while the connection is still coming up (e.g. mid-rollout),
	//     bounded by the call's own timeout/deadline.
	backendServiceConfig = `{
		"loadBalancingConfig": [{"round_robin": {}}],
		"methodConfig": [{"name": [{}], "waitForReady": true}]
	}`
)

// Function proxies RunFunctionRequests to a backend gRPC endpoint.
type Function struct {
	fnv1.UnimplementedFunctionRunnerServiceServer

	log logging.Logger

	connsMu  sync.RWMutex
	connPool map[string]*grpc.ClientConn
}

// NewFunction creates a new Function with an initialized connection pool.
func NewFunction(log logging.Logger) *Function {
	return &Function{
		log:      log,
		connPool: make(map[string]*grpc.ClientConn),
	}
}

// RunFunction decodes the proxy input, substitutes the payload, forwards the
// request to the backend, and returns the backend's response verbatim.
func (f *Function) RunFunction(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	f.log.Info("Running function proxy", "tag", req.GetMeta().GetTag())

	rsp := response.To(req, response.DefaultTTL)

	// Step 1: Parse and validate proxy input.
	in := &v1beta1.ProxyInput{}
	if err := request.GetInput(req, in); err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot parse ProxyInput"))
		return rsp, nil
	}

	if err := validateProxyInput(in); err != nil {
		response.Fatal(rsp, err)
		return rsp, nil
	}

	// Step 2: Marshal the payload to a structpb.Struct for the backend's input.
	payloadStruct, err := marshalPayload(in.Payload.Raw)
	if err != nil {
		response.Fatal(rsp, errors.Wrap(err, "cannot marshal payload to struct"))
		return rsp, nil
	}

	// Step 3: Normalize the backend URL into a gRPC target. This expands the
	// "service.namespace:port" shorthand to a fully qualified dns:/// target;
	// anything already carrying a scheme or an FQDN passes through unchanged.
	target := normalizeTarget(in.Backend.URL)

	f.log.Debug("Proxying to backend", "url", in.Backend.URL, "target", target, "tag", req.GetMeta().GetTag())

	// Step 4: Get or dial a connection to the backend, keyed by resolved target.
	conn, err := f.getConn(target)
	if err != nil {
		response.Fatal(rsp, errors.Wrapf(err, "cannot dial backend at %s", target))
		return rsp, nil
	}

	// Step 5: Substitute the input in place, then forward the original decoded
	// request. We deliberately mutate req and replace only Input rather than
	// rebuilding it field-by-field. Protobuf decodes any field this binary's
	// schema doesn't recognise into the message's unknown-field set and re-emits
	// it on marshal, so forwarding the original decoded message passes every
	// other RunFunctionRequest field through byte-faithfully — including fields
	// added to the Crossplane gRPC spec after this binary was built. Rebuilding
	// via &RunFunctionRequest{...} would silently drop those unknown fields and
	// force a proxy release just to forward them.
	req.Input = payloadStruct

	// Step 6: Apply timeout if specified, otherwise inherit from Crossplane.
	if in.Backend.Timeout != nil && in.Backend.Timeout.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, in.Backend.Timeout.Duration)
		defer cancel()
	}

	// Step 7: Forward to the backend and return its response verbatim. The
	// response is likewise the backend's decoded message, so its unknown fields
	// survive back to Crossplane unchanged.
	client := fnv1.NewFunctionRunnerServiceClient(conn)
	backendRsp, err := client.RunFunction(ctx, req,
		grpc.MaxCallRecvMsgSize(maxMsgSizeBytes),
		grpc.MaxCallSendMsgSize(maxMsgSizeBytes),
	)
	if err != nil {
		// Propagate gRPC errors so Crossplane retries the reconcile.
		f.log.Debug("Backend call failed", "url", in.Backend.URL, "error", err)
		return nil, err
	}

	return backendRsp, nil
}

// getConn returns a cached gRPC connection for the given target, dialing if needed.
// Connections are pooled by target URL and never evicted (MVP).
func (f *Function) getConn(target string) (*grpc.ClientConn, error) {
	f.connsMu.RLock()
	conn, ok := f.connPool[target]
	f.connsMu.RUnlock()

	if ok {
		return conn, nil
	}

	f.connsMu.Lock()
	defer f.connsMu.Unlock()

	// Double-check after acquiring write lock.
	if conn, ok = f.connPool[target]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(backendServiceConfig),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSizeBytes),
			grpc.MaxCallSendMsgSize(maxMsgSizeBytes),
		),
	)
	if err != nil {
		return nil, err
	}

	f.connPool[target] = conn
	return conn, nil
}

// CloseConnections closes all pooled gRPC connections. Used for cleanup in tests.
func (f *Function) CloseConnections() {
	f.connsMu.Lock()
	defer f.connsMu.Unlock()

	for target, conn := range f.connPool {
		if err := conn.Close(); err != nil {
			f.log.Debug("Error closing connection", "target", target, "error", err)
		}
	}
	f.connPool = make(map[string]*grpc.ClientConn)
}

// normalizeTarget converts a backend URL into a gRPC dial target, expanding the
// in-cluster "service.namespace:port" shorthand to a fully qualified name.
//
// Rules, in order:
//   - A URL that already carries a gRPC resolver scheme (contains "://", e.g.
//     "dns:///host:port", "unix:///path", "passthrough:///...") is returned
//     verbatim. This is the escape hatch for any target the shorthand would
//     otherwise mangle (e.g. an external two-label domain like "example.com").
//   - A "host:port" whose host is already fully qualified — an FQDN with the
//     cluster suffix, a trailing dot, or an IP address — is returned verbatim.
//   - A "service.namespace:port" (exactly two dot-separated host labels) is
//     expanded to "<service>.<namespace>.svc.cluster.local:port" following the
//     standard Kubernetes DNS order (first label = Service, second = namespace).
//   - Anything else (no port, single label, three+ labels not matching the
//     suffix, unparseable) is returned verbatim and left for gRPC to resolve or
//     reject. normalizeTarget never errors — it only rewrites the one shape it
//     recognises and otherwise stays out of the way.
func normalizeTarget(url string) string {
	// Already has an explicit scheme — caller opted out of shorthand expansion.
	if strings.Contains(url, "://") {
		return url
	}

	host, port, err := net.SplitHostPort(url)
	if err != nil {
		// No "host:port" shape (missing port, IPv6 without brackets, etc.).
		// Leave it for gRPC to interpret.
		return url
	}

	// Already fully qualified: cluster FQDN, rooted name, or IP literal.
	if strings.HasSuffix(host, "."+clusterDomainSuffix) ||
		strings.HasSuffix(host, ".") ||
		net.ParseIP(host) != nil {
		return url
	}

	// Expand only the exact two-label "service.namespace" shorthand.
	labels := strings.Split(host, ".")
	if len(labels) == 2 && labels[0] != "" && labels[1] != "" {
		fqdn := host + "." + clusterDomainSuffix
		return "dns:///" + net.JoinHostPort(fqdn, port)
	}

	// Single label, three+ labels, or empty labels: pass through untouched.
	return url
}

// validateProxyInput checks that the proxy input has the required fields.
func validateProxyInput(in *v1beta1.ProxyInput) error {
	if in.Backend.URL == "" {
		return errors.New("ProxyInput missing required field: backend.url")
	}
	if len(in.Payload.Raw) == 0 {
		return errors.New("ProxyInput missing required field: payload")
	}
	return nil
}

// marshalPayload converts a runtime.RawExtension's raw bytes to a structpb.Struct.
func marshalPayload(raw []byte) (*structpb.Struct, error) {
	if len(raw) == 0 {
		return nil, errors.New("payload is empty")
	}

	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, errors.Wrap(err, "cannot unmarshal payload")
	}

	return structpb.NewStruct(m)
}

// formatTimeout returns a human-readable duration string for logging.
func formatTimeout(d *time.Duration) string {
	if d == nil || *d == 0 {
		return "inherit"
	}
	return d.String()
}
