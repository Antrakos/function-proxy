package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	v1beta1 "github.com/Antrakos/function-proxy/input/v1beta1"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/crossplane/function-sdk-go/logging"
	fnv1 "github.com/crossplane/function-sdk-go/proto/v1"
	"github.com/crossplane/function-sdk-go/resource"
	"github.com/crossplane/function-sdk-go/response"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// mockBackend implements fnv1.FunctionRunnerServiceServer for testing.
type mockBackend struct {
	fnv1.UnimplementedFunctionRunnerServiceServer

	// handler is called when RunFunction is invoked. If nil, returns a default response.
	handler func(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error)

	// receivedReq captures the last request received by the mock.
	receivedReq *fnv1.RunFunctionRequest
}

func (m *mockBackend) RunFunction(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
	m.receivedReq = req
	if m.handler != nil {
		return m.handler(ctx, req)
	}
	rsp := response.To(req, response.DefaultTTL)
	response.Normalf(rsp, "mock backend processed request")
	return rsp, nil
}

// startBackendServer starts a gRPC server serving the given mock backend on a
// random localhost port. Returns the server, the listen address (host:port),
// and the gRPC target string suitable for dialing.
func startBackendServer(t *testing.T, mock *mockBackend) (*grpc.Server, string, string) {
	t.Helper()

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	fnv1.RegisterFunctionRunnerServiceServer(srv, mock)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Logf("Backend server stopped: %v", err)
		}
	}()

	addr := lis.Addr().String()
	return srv, addr, addr
}

// proxyInputStruct builds a ProxyInput structpb.Struct with the given backend URL,
// timeout, and payload.
func proxyInputStruct(backendURL, timeout string, payload map[string]interface{}) *structpb.Struct {
	input := map[string]interface{}{
		"apiVersion": "proxy.fn.antrakos.github.io/v1beta1",
		"kind":       "ProxyInput",
		"backend": map[string]interface{}{
			"url": backendURL,
		},
		"payload": payload,
	}
	if timeout != "" {
		input["backend"].(map[string]interface{})["timeout"] = timeout
	}

	s, err := structpb.NewStruct(input)
	if err != nil {
		panic(fmt.Sprintf("proxyInputStruct: failed to create struct: %v", err))
	}
	return s
}

// makeTestRequest creates a RunFunctionRequest with the given input and optional meta tag.
func makeTestRequest(tag string, input *structpb.Struct) *fnv1.RunFunctionRequest {
	return &fnv1.RunFunctionRequest{
		Meta:  &fnv1.RequestMeta{Tag: tag},
		Input: input,
	}
}

// ---------------------------------------------------------------------------
// Unit tests: validateProxyInput
// ---------------------------------------------------------------------------

func TestValidateProxyInput(t *testing.T) {
	cases := map[string]struct {
		input *v1beta1.ProxyInput
		want  string // expected error substring, empty means no error expected
	}{
		"ValidInput": {
			input: &v1beta1.ProxyInput{
				Backend: v1beta1.Backend{URL: "dns:///backend.svc:9443"},
				Payload: runtime.RawExtension{Raw: []byte(`{"key": "value"}`)},
			},
			want: "",
		},
		"MissingBackendURL": {
			input: &v1beta1.ProxyInput{
				Backend: v1beta1.Backend{URL: ""},
				Payload: runtime.RawExtension{Raw: []byte(`{"key": "value"}`)},
			},
			want: "backend.url",
		},
		"MissingPayload": {
			input: &v1beta1.ProxyInput{
				Backend: v1beta1.Backend{URL: "dns:///backend.svc:9443"},
				Payload: runtime.RawExtension{Raw: nil},
			},
			want: "payload",
		},
		"EmptyPayload": {
			input: &v1beta1.ProxyInput{
				Backend: v1beta1.Backend{URL: "dns:///backend.svc:9443"},
				Payload: runtime.RawExtension{Raw: []byte{}},
			},
			want: "payload",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateProxyInput(tc.input)
			if tc.want == "" && err != nil {
				t.Errorf("validateProxyInput(): expected no error, got %v", err)
			}
			if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Errorf("validateProxyInput(): expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests: marshalPayload
// ---------------------------------------------------------------------------

func TestMarshalPayload(t *testing.T) {
	cases := map[string]struct {
		payload []byte
		want    map[string]interface{}
		wantErr bool
	}{
		"ValidPayload": {
			payload: []byte(`{"apiVersion":"template.fn.crossplane.io/v1beta1","kind":"Input","key":"value"}`),
			want: map[string]interface{}{
				"apiVersion": "template.fn.crossplane.io/v1beta1",
				"kind":       "Input",
				"key":        "value",
			},
		},
		"NestedPayload": {
			payload: []byte(`{"outer":{"inner":"deep"},"list":[1,2,3]}`),
			want: map[string]interface{}{
				"outer": map[string]interface{}{"inner": "deep"},
				"list":  []interface{}{float64(1), float64(2), float64(3)},
			},
		},
		"EmptyPayload": {
			payload: []byte{},
			wantErr: true,
		},
		"NilPayload": {
			payload: nil,
			wantErr: true,
		},
		"InvalidJSON": {
			payload: []byte(`{not json}`),
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := marshalPayload(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Error("marshalPayload(): expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("marshalPayload(): unexpected error: %v", err)
				return
			}
			gotMap := got.AsMap()
			if diff := cmp.Diff(tc.want, gotMap); diff != "" {
				t.Errorf("marshalPayload(): -want, +got:\n%s", diff)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests: normalizeTarget
// ---------------------------------------------------------------------------

func TestNormalizeTarget(t *testing.T) {
	cases := map[string]struct {
		url  string
		want string
	}{
		// Shorthand expansion (the feature).
		"ServiceNamespaceShorthand": {
			url:  "redis.prod:9443",
			want: "dns:///redis.prod.svc.cluster.local:9443",
		},
		"ShorthandWithHyphens": {
			url:  "function-backend.example-system:9443",
			want: "dns:///function-backend.example-system.svc.cluster.local:9443",
		},
		// Already-qualified forms pass through verbatim.
		"ExplicitDNSScheme": {
			url:  "dns:///function-backend.example-system.svc.cluster.local:9443",
			want: "dns:///function-backend.example-system.svc.cluster.local:9443",
		},
		"ExplicitDNSSchemeOverShorthand": {
			// Escape hatch: external two-label domain forced verbatim via scheme.
			url:  "dns:///example.com:9443",
			want: "dns:///example.com:9443",
		},
		"UnixScheme": {
			url:  "unix:///var/run/backend.sock",
			want: "unix:///var/run/backend.sock",
		},
		"PassthroughScheme": {
			url:  "passthrough:///10.0.0.1:9443",
			want: "passthrough:///10.0.0.1:9443",
		},
		"FullClusterFQDN": {
			url:  "redis.prod.svc.cluster.local:9443",
			want: "redis.prod.svc.cluster.local:9443",
		},
		"RootedFQDN": {
			url:  "redis.prod.svc.cluster.local.:9443",
			want: "redis.prod.svc.cluster.local.:9443",
		},
		"IPv4Literal": {
			url:  "10.0.0.5:9443",
			want: "10.0.0.5:9443",
		},
		"IPv6Literal": {
			url:  "[::1]:9443",
			want: "[::1]:9443",
		},
		// Forms left untouched (not the recognised shorthand).
		"SingleLabelNoNamespace": {
			url:  "redis:9443",
			want: "redis:9443",
		},
		"ThreeLabelsNotClusterSuffix": {
			url:  "redis.prod.example:9443",
			want: "redis.prod.example:9443",
		},
		"NoPort": {
			url:  "redis.prod",
			want: "redis.prod",
		},
		"Empty": {
			url:  "",
			want: "",
		},
		"IngressHostNoPortNoScheme": {
			// Bare hostname, no port: gRPC's problem, not ours.
			url:  "backend.example.com",
			want: "backend.example.com",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := normalizeTarget(tc.url)
			if got != tc.want {
				t.Errorf("normalizeTarget(%q) = %q, want %q", tc.url, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Unit tests: formatTimeout
// ---------------------------------------------------------------------------

func TestFormatTimeout(t *testing.T) {
	cases := map[string]struct {
		d    *time.Duration
		want string
	}{
		"Nil":         {d: nil, want: "inherit"},
		"Zero":        {d: durationPtr(0), want: "inherit"},
		"FiveSeconds": {d: durationPtr(5 * time.Second), want: "5s"},
		"OneMinute":   {d: durationPtr(time.Minute), want: "1m0s"},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := formatTimeout(tc.d)
			if got != tc.want {
				t.Errorf("formatTimeout() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// End-to-end tests: proxy → mock backend over real gRPC
// ---------------------------------------------------------------------------

func TestRunFunction_ValidProxyInput(t *testing.T) {
	// Start a mock backend.
	mock := &mockBackend{}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	// Create proxy function.
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Build request with ProxyInput pointing to mock backend.
	req := makeTestRequest("test-tag", proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
		"composite":  "test-value",
	}))

	rsp, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Verify the response came from the mock backend.
	if rsp.GetMeta().GetTag() != "test-tag" {
		t.Errorf("RunFunction(): response tag = %q, want %q", rsp.GetMeta().GetTag(), "test-tag")
	}

	// Verify the backend received the substituted payload (not the ProxyInput).
	backendReq := mock.receivedReq
	if backendReq == nil {
		t.Fatal("Backend never received a request")
	}

	// The backend should have received the payload as its input, not the ProxyInput.
	payloadMap := backendReq.GetInput().AsMap()
	if payloadMap["apiVersion"] != "template.fn.crossplane.io/v1beta1" {
		t.Errorf("Backend received apiVersion = %q, want %q", payloadMap["apiVersion"], "template.fn.crossplane.io/v1beta1")
	}
	if payloadMap["kind"] != "Input" {
		t.Errorf("Backend received kind = %q, want %q", payloadMap["kind"], "Input")
	}
	if payloadMap["composite"] != "test-value" {
		t.Errorf("Backend received composite = %v, want %q", payloadMap["composite"], "test-value")
	}

	// Verify the ProxyInput fields are NOT in the backend's input.
	if _, ok := payloadMap["backend"]; ok {
		t.Error("Backend received 'backend' field in input — ProxyInput was not properly substituted")
	}
	if _, ok := payloadMap["payload"]; ok {
		t.Error("Backend received 'payload' field in input — ProxyInput was not properly substituted")
	}
}

func TestRunFunction_ResponsePassthrough(t *testing.T) {
	// Start a mock backend that returns a specific response with desired resources.
	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			rsp.Desired = &fnv1.State{
				Composite: &fnv1.Resource{
					Resource: resource.MustStructJSON(`{"apiVersion":"example.org/v1","kind":"XR","spec":{"replicas":3}}`),
				},
			}
			rsp.Results = append(rsp.Results, &fnv1.Result{
				Severity: fnv1.Severity_SEVERITY_NORMAL,
				Message:  "backend created 3 replicas",
			})
			rsp.Context, _ = structpb.NewStruct(map[string]interface{}{
				"backend-ctx": "preserved",
			})
			return rsp, nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	req := makeTestRequest("passthrough-tag", proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	}))

	rsp, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Verify the backend's response is returned verbatim.
	if rsp.GetMeta().GetTag() != "passthrough-tag" {
		t.Errorf("Response tag = %q, want %q", rsp.GetMeta().GetTag(), "passthrough-tag")
	}

	// Verify desired state passed through.
	compositeSpec := rsp.GetDesired().GetComposite().GetResource().AsMap()
	if compositeSpec["spec"] == nil {
		t.Error("Response missing desired composite resource")
	}

	// Verify results passed through.
	if len(rsp.GetResults()) == 0 {
		t.Error("Response missing results from backend")
	}

	// Verify context passed through.
	ctxVal := rsp.GetContext().AsMap()
	if ctxVal["backend-ctx"] != "preserved" {
		t.Errorf("Response context = %v, want 'preserved'", ctxVal["backend-ctx"])
	}
}

func TestRunFunction_ConnectionPooling(t *testing.T) {
	mock := &mockBackend{}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	input := proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	// First call should dial a new connection.
	_, err := f.RunFunction(context.Background(), makeTestRequest("call-1", input))
	if err != nil {
		t.Fatalf("RunFunction() call 1: unexpected error: %v", err)
	}

	// Second call should reuse the existing connection.
	_, err = f.RunFunction(context.Background(), makeTestRequest("call-2", input))
	if err != nil {
		t.Fatalf("RunFunction() call 2: unexpected error: %v", err)
	}

	// Verify only one connection was created for the target.
	f.connsMu.RLock()
	connCount := len(f.connPool)
	f.connsMu.RUnlock()

	if connCount != 1 {
		t.Errorf("Connection pool size = %d, want 1", connCount)
	}
}

// TestRunFunction_PoolKeyedByNormalizedTarget verifies the connection pool is
// keyed by the normalized gRPC target, not the raw backend URL. The shorthand
// "service.namespace:port" must produce a "dns:///...svc.cluster.local:port"
// pool entry. We use a non-resolvable shorthand and tolerate the dial outcome —
// the assertion is purely about the pool key.
func TestRunFunction_PoolKeyedByNormalizedTarget(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// A short timeout keeps the doomed RPC to the non-resolvable name from
	// stalling the test; the pool entry is created at dial time, before the RPC.
	input := proxyInputStruct("backend.testns:9443", "50ms", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, _ = f.RunFunction(context.Background(), makeTestRequest("shorthand", input))

	f.connsMu.RLock()
	defer f.connsMu.RUnlock()

	const wantKey = "dns:///backend.testns.svc.cluster.local:9443"
	if _, ok := f.connPool[wantKey]; !ok {
		t.Errorf("Connection pool missing expected normalized key %q; keys present: %v", wantKey, poolKeys(f))
	}
	if _, ok := f.connPool["backend.testns:9443"]; ok {
		t.Error("Connection pool contains the raw shorthand key — target was not normalized before pooling")
	}
}

func poolKeys(f *Function) []string {
	keys := make([]string, 0, len(f.connPool))
	for k := range f.connPool {
		keys = append(keys, k)
	}
	return keys
}

func TestRunFunction_MissingBackendURL(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput with empty backend URL.
	input := proxyInputStruct("", "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	rsp, err := f.RunFunction(context.Background(), makeTestRequest("no-url", input))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected gRPC error: %v", err)
	}

	// Should return a FATAL result, not a gRPC error.
	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for missing backend.url, but got none")
	}
}

func TestRunFunction_MissingPayload(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput with backend URL but no payload.
	inputMap := map[string]interface{}{
		"apiVersion": "proxy.fn.antrakos.github.io/v1beta1",
		"kind":       "ProxyInput",
		"backend": map[string]interface{}{
			"url": "dns:///backend.svc:9443",
		},
		// No payload field.
	}
	s, _ := structpb.NewStruct(inputMap)

	rsp, err := f.RunFunction(context.Background(), makeTestRequest("no-payload", s))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected gRPC error: %v", err)
	}

	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for missing payload, but got none")
	}
}

func TestRunFunction_InvalidPayload(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput with invalid JSON payload (string instead of object).
	inputMap := map[string]interface{}{
		"apiVersion": "proxy.fn.antrakos.github.io/v1beta1",
		"kind":       "ProxyInput",
		"backend": map[string]interface{}{
			"url": "dns:///backend.svc:9443",
		},
		"payload": "not-a-valid-json-object",
	}
	s, _ := structpb.NewStruct(inputMap)

	rsp, err := f.RunFunction(context.Background(), makeTestRequest("bad-payload", s))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected gRPC error: %v", err)
	}

	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for invalid payload, but got none")
	}
}

func TestRunFunction_MalformedInput(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Request with input that doesn't match ProxyInput schema at all.
	input, _ := structpb.NewStruct(map[string]interface{}{
		"apiVersion": "unknown.io/v1",
		"kind":       "Something",
		"random":     "data",
	})

	rsp, err := f.RunFunction(context.Background(), makeTestRequest("malformed", input))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected gRPC error: %v", err)
	}

	// With no backend.url and no payload, should get FATAL.
	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for malformed input, but got none")
	}
}

func TestRunFunction_NilInput(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	req := &fnv1.RunFunctionRequest{
		Meta:  &fnv1.RequestMeta{Tag: "nil-input"},
		Input: nil,
	}

	rsp, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected gRPC error: %v", err)
	}

	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for nil input, but got none")
	}
}

func TestRunFunction_BackendDialFailure(t *testing.T) {
	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Use a port that nothing is listening on. Because the proxy dials with
	// waitForReady, an unreachable backend does not fail fast — the call waits
	// for a backend that never becomes ready, so it must be bounded by a
	// deadline. In production Crossplane always supplies one; here we set a short
	// backend timeout to stand in for it. A never-ready backend therefore yields
	// DeadlineExceeded rather than an immediate connection-refused error.
	input := proxyInputStruct("localhost:1", "200ms", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	rsp, err := f.RunFunction(context.Background(), makeTestRequest("dial-fail", input))
	if err != nil {
		// A gRPC error (DeadlineExceeded, or Unavailable in some dial modes) is
		// the expected outcome — it propagates so Crossplane retries.
		return
	}

	// If we got a response instead, it should carry a FATAL result.
	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for backend dial failure, but got none")
	}
}

func TestRunFunction_BackendCallError(t *testing.T) {
	// Start a mock backend that returns a gRPC error.
	mock := &mockBackend{
		handler: func(_ context.Context, _ *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			return nil, status.Error(codes.Unavailable, "backend is overloaded")
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	input := proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, err := f.RunFunction(context.Background(), makeTestRequest("backend-error", input))
	if err == nil {
		t.Fatal("RunFunction(): expected gRPC error from backend, got nil")
	}

	// Verify the error is a gRPC status error.
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("RunFunction(): expected gRPC status error, got %T: %v", err, err)
	}

	if st.Code() != codes.Unavailable {
		t.Errorf("RunFunction(): error code = %v, want Unavailable", st.Code())
	}
}

func TestRunFunction_BackendTimeout(t *testing.T) {
	// Start a mock backend that sleeps before responding.
	mock := &mockBackend{
		handler: func(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			select {
			case <-time.After(5 * time.Second):
				rsp := response.To(req, response.DefaultTTL)
				return rsp, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Set a very short timeout.
	input := proxyInputStruct(target, "1ms", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, err := f.RunFunction(context.Background(), makeTestRequest("timeout", input))
	if err == nil {
		t.Fatal("RunFunction(): expected timeout error, got nil")
	}

	// The error should be a deadline exceeded or unavailable.
	st, ok := status.FromError(err)
	if !ok {
		// Could be a context.DeadlineExceeded wrapped differently.
		t.Logf("RunFunction() error (non-status): %v", err)
		return
	}

	if st.Code() != codes.DeadlineExceeded && st.Code() != codes.Unavailable {
		t.Errorf("RunFunction(): error code = %v, want DeadlineExceeded or Unavailable", st.Code())
	}
}

func TestRunFunction_RequestFieldsPreserved(t *testing.T) {
	// Verify that all RunFunctionRequest fields (except Input) are passed through to the backend.
	var capturedReq *fnv1.RunFunctionRequest

	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			capturedReq = req
			rsp := response.To(req, response.DefaultTTL)
			return rsp, nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Build a request with all fields populated.
	observedComposite, _ := structpb.NewStruct(map[string]interface{}{
		"apiVersion": "example.org/v1",
		"kind":       "XR",
		"metadata":   map[string]interface{}{"name": "test-xr"},
		"spec":       map[string]interface{}{"param": "value"},
	})
	desiredComposite, _ := structpb.NewStruct(map[string]interface{}{
		"apiVersion": "example.org/v1",
		"kind":       "XR",
		"spec":       map[string]interface{}{"desiredParam": "desiredValue"},
	})
	reqCtx, _ := structpb.NewStruct(map[string]interface{}{
		"pipeline-ctx": "from-previous-function",
	})

	req := &fnv1.RunFunctionRequest{
		Meta: &fnv1.RequestMeta{Tag: "preserve-fields"},
		Observed: &fnv1.State{
			Composite: &fnv1.Resource{
				Resource: observedComposite,
			},
		},
		Desired: &fnv1.State{
			Composite: &fnv1.Resource{
				Resource: desiredComposite,
			},
		},
		Input: proxyInputStruct(target, "", map[string]interface{}{
			"apiVersion": "template.fn.crossplane.io/v1beta1",
			"kind":       "Input",
		}),
		Context: reqCtx,
		Credentials: map[string]*fnv1.Credentials{
			"source": {
				Source: &fnv1.Credentials_CredentialData{
					CredentialData: &fnv1.CredentialData{
						Data: map[string][]byte{
							"token": []byte("secret-value"),
						},
					},
				},
			},
		},
	}

	_, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Verify all fields (except Input) are preserved in the forwarded request.
	if capturedReq.GetMeta().GetTag() != "preserve-fields" {
		t.Errorf("Meta.Tag not preserved: got %q", capturedReq.GetMeta().GetTag())
	}

	if capturedReq.GetObserved().GetComposite().GetResource().AsMap()["spec"] == nil {
		t.Error("Observed composite not preserved in forwarded request")
	}

	if capturedReq.GetDesired().GetComposite().GetResource().AsMap()["spec"] == nil {
		t.Error("Desired composite not preserved in forwarded request")
	}

	ctxMap := capturedReq.GetContext().AsMap()
	if ctxMap["pipeline-ctx"] != "from-previous-function" {
		t.Errorf("Context not preserved: got %v", ctxMap["pipeline-ctx"])
	}

	if len(capturedReq.GetCredentials()) == 0 {
		t.Error("Credentials not preserved in forwarded request")
	}
}

func TestRunFunction_BackendReceivesSubstitutedInput(t *testing.T) {
	var capturedReq *fnv1.RunFunctionRequest

	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			capturedReq = req
			return response.To(req, response.DefaultTTL), nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// The payload is a template Input with nested objects.
	payloadContent := map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
		"composite": map[string]interface{}{
			"resource": map[string]interface{}{
				"apiVersion": "database.example.org/v1",
				"kind":       "PostgreSQL",
				"spec": map[string]interface{}{
					"version": "15",
				},
			},
		},
	}

	req := makeTestRequest("sub-test", proxyInputStruct(target, "", payloadContent))

	_, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Verify the backend received the payload as its input.
	gotInput := capturedReq.GetInput().AsMap()

	// The backend should NOT receive ProxyInput fields.
	if _, ok := gotInput["backend"]; ok {
		t.Error("Backend received 'backend' field — input substitution failed")
	}
	if _, ok := gotInput["payload"]; ok {
		t.Error("Backend received 'payload' field — input substitution failed")
	}

	// The backend SHOULD receive the payload contents.
	if gotInput["apiVersion"] != "template.fn.crossplane.io/v1beta1" {
		t.Errorf("Backend apiVersion = %v, want template.fn.crossplane.io/v1beta1", gotInput["apiVersion"])
	}
	if gotInput["kind"] != "Input" {
		t.Errorf("Backend kind = %v, want Input", gotInput["kind"])
	}
}

// TestRunFunction_UnknownFieldsPreserved is the regression guard for forward
// compatibility: a field added to the Crossplane RunFunctionRequest spec after
// this binary was compiled must still reach the backend. We simulate such a
// field by appending a wire-encoded field with a number this binary's schema
// does not define, decoding it into a RunFunctionRequest (where it lands in the
// unknown-field set), and asserting it survives the proxy hop to the backend.
func TestRunFunction_UnknownFieldsPreserved(t *testing.T) {
	var capturedRaw []byte

	// The mock captures the raw bytes it received off the wire so we can inspect
	// unknown fields the generated struct would otherwise hide.
	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			b, err := proto.Marshal(req)
			if err != nil {
				return nil, err
			}
			capturedRaw = b
			return response.To(req, response.DefaultTTL), nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Build a normal request, marshal it, then append an unknown field. Field
	// number 99999 is not assigned in the RunFunctionRequest schema, so on
	// decode it becomes an unknown field. We use a length-delimited (wire type 2)
	// value carrying a recognizable marker string.
	base := makeTestRequest("unknown-fields", proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	}))
	baseBytes, err := proto.Marshal(base)
	if err != nil {
		t.Fatalf("Failed to marshal base request: %v", err)
	}

	marker := []byte("future-crossplane-field")
	var unknown []byte
	unknown = protowire.AppendTag(unknown, 99999, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, marker)
	withUnknown := append(append([]byte(nil), baseBytes...), unknown...)

	// Decode back into a RunFunctionRequest: the unknown field is retained in the
	// message's unknown-field set, exactly as it would be for a genuinely newer
	// Crossplane.
	req := &fnv1.RunFunctionRequest{}
	if err := proto.Unmarshal(withUnknown, req); err != nil {
		t.Fatalf("Failed to unmarshal request with unknown field: %v", err)
	}

	if _, err := f.RunFunction(context.Background(), req); err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// The backend must have received the unknown field bytes intact.
	if !bytes.Contains(capturedRaw, marker) {
		t.Error("Unknown field was dropped by the proxy — forward compatibility broken")
	}
}

// TestRunFunction_RoundRobinAcrossReplicas verifies the proxy spreads calls
// across multiple backend endpoints, the way a multi-replica headless Service
// resolves. We register two backend servers under a single manual-resolver
// target (standing in for the multiple A records a headless Service returns)
// and assert both receive traffic — which only happens with round_robin, not
// gRPC's default pick_first.
func TestRunFunction_RoundRobinAcrossReplicas(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}
	makeMock := func(id string) *mockBackend {
		return &mockBackend{
			handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
				mu.Lock()
				hits[id]++
				mu.Unlock()
				return response.To(req, response.DefaultTTL), nil
			},
		}
	}

	srv1, addr1, _ := startBackendServer(t, makeMock("pod-1"))
	defer srv1.Stop()
	srv2, addr2, _ := startBackendServer(t, makeMock("pod-2"))
	defer srv2.Stop()

	// A manual resolver returns both endpoints under one target, exactly like a
	// headless Service's DNS returning one A record per ready pod.
	r := manual.NewBuilderWithScheme("replicas")
	r.InitialState(grpcresolver.State{Addresses: []grpcresolver.Address{
		{Addr: addr1}, {Addr: addr2},
	}})
	target := r.Scheme() + ":///backend"

	conn, err := grpc.NewClient(target,
		grpc.WithResolvers(r),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(backendServiceConfig),
	)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	client := fnv1.NewFunctionRunnerServiceClient(conn)
	for i := range 20 {
		if _, err := client.RunFunction(context.Background(), makeTestRequest("rr", nil)); err != nil {
			t.Fatalf("RunFunction() call %d: unexpected error: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if hits["pod-1"] == 0 || hits["pod-2"] == 0 {
		t.Errorf("round_robin did not spread across replicas: %v — both pods should receive traffic", hits)
	}
}

func TestRunFunction_MultipleBackends(t *testing.T) {
	// Two different backends.
	mock1 := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			response.Normalf(rsp, "backend-1")
			return rsp, nil
		},
	}
	mock2 := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			response.Normalf(rsp, "backend-2")
			return rsp, nil
		},
	}

	srv1, _, target1 := startBackendServer(t, mock1)
	defer srv1.Stop()
	srv2, _, target2 := startBackendServer(t, mock2)
	defer srv2.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// Call backend 1.
	rsp1, err := f.RunFunction(context.Background(), makeTestRequest("call-1",
		proxyInputStruct(target1, "", map[string]interface{}{"kind": "Input1"})))
	if err != nil {
		t.Fatalf("RunFunction() backend 1: unexpected error: %v", err)
	}

	// Call backend 2.
	rsp2, err := f.RunFunction(context.Background(), makeTestRequest("call-2",
		proxyInputStruct(target2, "", map[string]interface{}{"kind": "Input2"})))
	if err != nil {
		t.Fatalf("RunFunction() backend 2: unexpected error: %v", err)
	}

	// Verify different backends responded.
	msg1 := rsp1.GetResults()[0].GetMessage()
	msg2 := rsp2.GetResults()[0].GetMessage()
	if msg1 == msg2 {
		t.Errorf("Both backends returned same message: %q — routing may be broken", msg1)
	}

	// Verify two connections in pool.
	f.connsMu.RLock()
	connCount := len(f.connPool)
	f.connsMu.RUnlock()

	if connCount != 2 {
		t.Errorf("Connection pool size = %d, want 2", connCount)
	}
}

func TestRunFunction_TimeoutInherited(t *testing.T) {
	// When timeout is 0 or not set, the proxy should not add its own deadline.
	var hasDeadline bool

	mock := &mockBackend{
		handler: func(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			_, hasDeadline = ctx.Deadline()
			return response.To(req, response.DefaultTTL), nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput with timeout: 0s (inherit).
	input := proxyInputStruct(target, "0s", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, err := f.RunFunction(context.Background(), makeTestRequest("inherit-timeout", input))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Context should NOT have a deadline (inherited from Crossplane, not set by proxy).
	if hasDeadline {
		t.Error("Context has a deadline when timeout=0s — should inherit Crossplane's timeout")
	}
}

func TestRunFunction_TimeoutNotSet(t *testing.T) {
	// When timeout is omitted entirely, the proxy should not add its own deadline.
	var hasDeadline bool

	mock := &mockBackend{
		handler: func(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			_, hasDeadline = ctx.Deadline()
			return response.To(req, response.DefaultTTL), nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput without timeout field.
	input := proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, err := f.RunFunction(context.Background(), makeTestRequest("no-timeout", input))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Context should NOT have a deadline.
	if hasDeadline {
		t.Error("Context has a deadline when timeout is omitted — should inherit Crossplane's timeout")
	}
}

func TestRunFunction_TimeoutSet(t *testing.T) {
	var (
		deadline    time.Time
		hasDeadline bool
	)

	mock := &mockBackend{
		handler: func(ctx context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			deadline, hasDeadline = ctx.Deadline()
			return response.To(req, response.DefaultTTL), nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	// ProxyInput with 5s timeout.
	input := proxyInputStruct(target, "5s", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	_, err := f.RunFunction(context.Background(), makeTestRequest("set-timeout", input))
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Context should have a deadline of approximately 5 seconds from now.
	if !hasDeadline {
		t.Fatal("Context has no deadline when timeout=5s")
	}
	remaining := time.Until(deadline)
	if remaining < 3*time.Second || remaining > 6*time.Second {
		t.Errorf("Context deadline remaining = %v, want ~5s", remaining)
	}
}

// ---------------------------------------------------------------------------
// E2E test: full gRPC round trip client → proxy server → backend server
// ---------------------------------------------------------------------------

func TestRunFunction_E2EViaGRPC(t *testing.T) {
	// Start a mock backend.
	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			rsp.Desired = &fnv1.State{
				Composite: &fnv1.Resource{
					Resource: resource.MustStructJSON(`{"apiVersion":"example.org/v1","kind":"XR","spec":{"provisioned":true}}`),
				},
			}
			response.Normalf(rsp, "backend processed request with tag %s", req.GetMeta().GetTag())
			return rsp, nil
		},
	}
	backendSrv, _, backendTarget := startBackendServer(t, mock)
	defer backendSrv.Stop()

	// Start the function-proxy server.
	lc := net.ListenConfig{}
	proxyLis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for proxy: %v", err)
	}

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	proxySrv := grpc.NewServer()
	fnv1.RegisterFunctionRunnerServiceServer(proxySrv, f)

	go func() {
		if err := proxySrv.Serve(proxyLis); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxySrv.Stop()

	proxyAddr := proxyLis.Addr().String()

	// Dial the proxy as a client.
	conn, err := grpc.NewClient(proxyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial proxy: %v", err)
	}
	defer conn.Close()

	client := fnv1.NewFunctionRunnerServiceClient(conn)

	// Build a request that routes to the mock backend.
	req := &fnv1.RunFunctionRequest{
		Meta: &fnv1.RequestMeta{Tag: "e2e-test"},
		Input: proxyInputStruct(backendTarget, "", map[string]interface{}{
			"apiVersion": "template.fn.crossplane.io/v1beta1",
			"kind":       "Input",
			"composite":  "e2e-value",
		}),
	}

	rsp, err := client.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("E2E RunFunction(): unexpected error: %v", err)
	}

	// Verify the response came through the proxy from the backend.
	if rsp.GetMeta().GetTag() != "e2e-test" {
		t.Errorf("E2E response tag = %q, want %q", rsp.GetMeta().GetTag(), "e2e-test")
	}

	// Verify desired state from the backend.
	if rsp.GetDesired() == nil || rsp.GetDesired().GetComposite() == nil {
		t.Fatal("E2E response missing desired composite resource")
	}

	desiredMap := rsp.GetDesired().GetComposite().GetResource().AsMap()
	spec, ok := desiredMap["spec"].(map[string]interface{})
	if !ok || !spec["provisioned"].(bool) {
		t.Errorf("E2E response desired composite spec.provisioned = %v, want true", spec["provisioned"])
	}

	// Verify the backend received the substituted input.
	if mock.receivedReq == nil {
		t.Fatal("Backend never received request from proxy")
	}
	backendInput := mock.receivedReq.GetInput().AsMap()
	if backendInput["composite"] != "e2e-value" {
		t.Errorf("Backend received composite = %v, want 'e2e-value'", backendInput["composite"])
	}
}

func TestRunFunction_E2EBackendError(t *testing.T) {
	// Start a mock backend that returns an error.
	mock := &mockBackend{
		handler: func(_ context.Context, _ *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			return nil, status.Error(codes.Internal, "something broke")
		},
	}
	backendSrv, _, backendTarget := startBackendServer(t, mock)
	defer backendSrv.Stop()

	// Start the proxy.
	lc := net.ListenConfig{}
	proxyLis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for proxy: %v", err)
	}

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	proxySrv := grpc.NewServer()
	fnv1.RegisterFunctionRunnerServiceServer(proxySrv, f)

	go func() {
		if err := proxySrv.Serve(proxyLis); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxySrv.Stop()

	conn, err := grpc.NewClient(proxyLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial proxy: %v", err)
	}
	defer conn.Close()

	client := fnv1.NewFunctionRunnerServiceClient(conn)

	req := &fnv1.RunFunctionRequest{
		Meta: &fnv1.RequestMeta{Tag: "e2e-error"},
		Input: proxyInputStruct(backendTarget, "", map[string]interface{}{
			"kind": "Input",
		}),
	}

	_, err = client.RunFunction(context.Background(), req)
	if err == nil {
		t.Fatal("E2E RunFunction(): expected error from backend, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("E2E RunFunction(): expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != codes.Internal {
		t.Errorf("E2E RunFunction(): error code = %v, want Internal", st.Code())
	}
}

// ---------------------------------------------------------------------------
// E2E test: proxy input with no payload produces FATAL (verifying end-to-end
// error handling through the full gRPC stack)
// ---------------------------------------------------------------------------

func TestRunFunction_E2EMalformedInput(t *testing.T) {
	// Start the proxy.
	lc := net.ListenConfig{}
	proxyLis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to listen for proxy: %v", err)
	}

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	proxySrv := grpc.NewServer()
	fnv1.RegisterFunctionRunnerServiceServer(proxySrv, f)

	go func() {
		if err := proxySrv.Serve(proxyLis); err != nil {
			t.Logf("Proxy server stopped: %v", err)
		}
	}()
	defer proxySrv.Stop()

	conn, err := grpc.NewClient(proxyLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to dial proxy: %v", err)
	}
	defer conn.Close()

	client := fnv1.NewFunctionRunnerServiceClient(conn)

	// Send a request with malformed input (no backend, no payload).
	badInput, _ := structpb.NewStruct(map[string]interface{}{
		"apiVersion": "unknown.io/v1",
		"kind":       "Something",
	})

	req := &fnv1.RunFunctionRequest{
		Meta:  &fnv1.RequestMeta{Tag: "e2e-malformed"},
		Input: badInput,
	}

	rsp, err := client.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("E2E RunFunction(): unexpected error: %v", err)
	}

	// Should get FATAL result in the response, not a gRPC error.
	if !hasFatalResult(rsp) {
		t.Error("Expected FATAL result for malformed input via E2E, but got none")
	}
}

// ---------------------------------------------------------------------------
// Proto comparison test to ensure responses are structurally correct
// ---------------------------------------------------------------------------

func TestRunFunction_ResponseStructure(t *testing.T) {
	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			response.Normalf(rsp, "test message")
			return rsp, nil
		},
	}
	srv, _, target := startBackendServer(t, mock)
	defer srv.Stop()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	req := makeTestRequest("struct-test", proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	}))

	rsp, err := f.RunFunction(context.Background(), req)
	if err != nil {
		t.Fatalf("RunFunction(): unexpected error: %v", err)
	}

	// Verify the response structure using proto comparison.
	wantMeta := &fnv1.ResponseMeta{
		Tag: "struct-test",
		Ttl: durationpb.New(response.DefaultTTL),
	}

	wantResults := []*fnv1.Result{
		{
			Severity: fnv1.Severity_SEVERITY_NORMAL,
			Message:  "test message",
			Target:   fnv1.Target_TARGET_COMPOSITE.Enum(),
		},
	}

	if diff := cmp.Diff(wantMeta, rsp.GetMeta(), protocmp.Transform()); diff != "" {
		t.Errorf("Response Meta: -want, +got:\n%s", diff)
	}

	if diff := cmp.Diff(wantResults, rsp.GetResults(), protocmp.Transform()); diff != "" {
		t.Errorf("Response Results: -want, +got:\n%s", diff)
	}
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

func BenchmarkRunFunction(b *testing.B) {
	mock := &mockBackend{
		handler: func(_ context.Context, req *fnv1.RunFunctionRequest) (*fnv1.RunFunctionResponse, error) {
			rsp := response.To(req, response.DefaultTTL)
			return rsp, nil
		},
	}

	lc := net.ListenConfig{}
	lis, err := lc.Listen(context.Background(), "tcp", "localhost:0")
	if err != nil {
		b.Fatalf("Failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	fnv1.RegisterFunctionRunnerServiceServer(srv, mock)

	go func() {
		if err := srv.Serve(lis); err != nil {
			b.Logf("Backend server stopped: %v", err)
		}
	}()
	defer srv.Stop()

	target := lis.Addr().String()

	f := NewFunction(logging.NewNopLogger())
	defer f.CloseConnections()

	input := proxyInputStruct(target, "", map[string]interface{}{
		"apiVersion": "template.fn.crossplane.io/v1beta1",
		"kind":       "Input",
	})

	req := makeTestRequest("bench", input)
	ctx := context.Background()

	b.ResetTimer()

	for range b.N {
		if _, err := f.RunFunction(ctx, req); err != nil {
			b.Fatalf("RunFunction(): unexpected error: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func hasFatalResult(rsp *fnv1.RunFunctionResponse) bool {
	for _, r := range rsp.GetResults() {
		if r.GetSeverity() == fnv1.Severity_SEVERITY_FATAL {
			return true
		}
	}
	return false
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

// Compile-time interface check.
var _ fnv1.FunctionRunnerServiceServer = &Function{}
