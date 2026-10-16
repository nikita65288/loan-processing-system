package com.lps.notification.outbox;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import java.util.Map;

/** Durable W3C parent pointer for delayed Outbox publication, independent of thread-local state. */
public final class OutboxTraceContext {
    private OutboxTraceContext() { }

    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Map<String, String> carrier) { return carrier.keySet(); }
        @Override public String get(Map<String, String> carrier, String key) { return carrier.get(key); }
    };

    /** Optional at insert time: legacy/out-of-band records are allowed to have no active span. */
    public static String capture() {
        SpanContext context = Span.current().getSpanContext();
        if (!context.isValid()) return null;
        return "00-" + context.getTraceId() + "-" + context.getSpanId() + "-" + context.getTraceFlags().asHex();
    }

    /** Restore the persisted remote parent before the observed KafkaTemplate creates a send span. */
    public static Scope resume(String traceparent) {
        if (traceparent == null || traceparent.isBlank()) return Context.current().makeCurrent();
        return W3CTraceContextPropagator.getInstance()
                .extract(Context.root(), Map.of("traceparent", traceparent), GETTER)
                .makeCurrent();
    }
}
