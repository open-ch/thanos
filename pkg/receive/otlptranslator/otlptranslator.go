package otlptranslator

import "go.opentelemetry.io/otel"

var tracer = otel.Tracer("github.com/thanos-io/thanos/pkg/receive/otlptranslator")
