// Package v1beta1 contains the input type for this Function.
// +kubebuilder:object:generate=true
// +groupName=proxy.fn.antrakos.github.io
// +versionName=v1beta1
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ProxyInput can be used to provide input to this Function.
// It specifies the backend to forward to and the payload to substitute as the backend's input.
// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:resource:categories=crossplane
type ProxyInput struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Backend specifies the gRPC endpoint to forward the request to.
	// The proxy always dials insecure h2c; the backend must run with --insecure.
	// +kubebuilder:validation:Required
	Backend Backend `json:"backend"`

	// Payload is the opaque KRM object forwarded as the backend's input.
	// The proxy treats this as opaque and does not interpret its contents.
	// +kubebuilder:validation:Required
	Payload runtime.RawExtension `json:"payload"`
}

// Backend specifies the gRPC endpoint that the proxy forwards requests to.
type Backend struct {
	// URL is the gRPC target to dial. Supported forms:
	//   - "service.namespace:port" shorthand — expanded to the in-cluster FQDN
	//     "dns:///service.namespace.svc.cluster.local:port". Follows Kubernetes
	//     DNS order: first label is the Service, second is the namespace.
	//   - A full host:port (e.g. an FQDN, IP, or Ingress host), used verbatim.
	//   - An explicit gRPC resolver target (anything containing "://", e.g.
	//     "dns:///host:port" or "unix:///path"), used verbatim. Use this form to
	//     opt out of shorthand expansion — e.g. an external two-label domain like
	//     "dns:///example.com:9443".
	// Examples:
	//   function-backend.example-system:9443
	//   dns:///function-backend.example-system.svc.cluster.local:9443
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`

	// Timeout for the backend call. 0s (default) inherits Crossplane's function timeout.
	// +kubebuilder:validation:Optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}
