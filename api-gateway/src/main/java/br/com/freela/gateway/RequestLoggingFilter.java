package br.com.freela.gateway;

import io.micrometer.observation.Observation;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.tracing.handler.TracingObservationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.UUID;

@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {
    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    private record Trace(String traceId, String spanId) {
        static final Trace VAZIO = new Trace("", "");
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst("X-Correlation-Id");
        if (correlationId == null || correlationId.isBlank()) correlationId = UUID.randomUUID().toString();
        String finalCorrelationId = correlationId;
        long inicio = System.currentTimeMillis();
        var request = exchange.getRequest().mutate().header("X-Correlation-Id", correlationId).build();

        return Mono.deferContextual(ctx -> {
            Trace trace = traceAtual(ctx);
            comContexto(finalCorrelationId, trace, () -> log.info("gateway.request.inicio correlationId={} method={} path={}",
                    finalCorrelationId, request.getMethod(), request.getURI().getPath()));

            return chain.filter(exchange.mutate().request(request).build())
                    .doFinally(signal -> comContexto(finalCorrelationId, trace, () -> log.info(
                            "gateway.request.fim correlationId={} method={} path={} status={} durationMs={} signal={}",
                            finalCorrelationId, request.getMethod(), request.getURI().getPath(),
                            exchange.getResponse().getStatusCode(), System.currentTimeMillis() - inicio, signal)));
        });
    }

    // No WebFlux a Observation da requisição vive no contexto do Reactor, não em thread-local.
    private static Trace traceAtual(ContextView ctx) {
        Observation observation = ctx.getOrDefault(ObservationThreadLocalAccessor.KEY, null);
        if (observation == null) return Trace.VAZIO;
        TracingObservationHandler.TracingContext tracing =
                observation.getContext().get(TracingObservationHandler.TracingContext.class);
        if (tracing == null || tracing.getSpan() == null) return Trace.VAZIO;
        return new Trace(tracing.getSpan().context().traceId(), tracing.getSpan().context().spanId());
    }

    // A requisição troca de thread ao longo do fluxo; o MDC é definido só em volta de cada log.
    private static void comContexto(String correlationId, Trace trace, Runnable acao) {
        MDC.put("correlationId", correlationId);
        MDC.put("traceId", trace.traceId());
        MDC.put("spanId", trace.spanId());
        try {
            acao.run();
        } finally {
            MDC.remove("correlationId");
            MDC.remove("traceId");
            MDC.remove("spanId");
        }
    }

    @Override public int getOrder() { return -100; }
}
