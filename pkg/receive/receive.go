package receive

import "go.opentelemetry.io/otel"

var (
	tracer = otel.Tracer("github.com/thanos-io/thanos/pkg/receive")
)
