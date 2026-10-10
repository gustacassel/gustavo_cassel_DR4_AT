package br.com.freela.contrato.infrastructure.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {

    private static final String TOPICO = "contratos.eventos";

    private final SpringDataOutboxRepository repository = mock(SpringDataOutboxRepository.class);
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final Tracer tracer = mock(Tracer.class, RETURNS_DEEP_STUBS);
    private final OutboxPublisher publisher =
            new OutboxPublisher(repository, kafkaTemplate, new ObjectMapper(), tracer, TOPICO);

    private final UUID contratoId = UUID.randomUUID();

    @BeforeEach
    void tracerSemEfeito() {
        Span span = mock(Span.class);
        when(tracer.spanBuilder().name(any()).tag(any(), any()).tag(any(), any()).tag(any(), any()).start()).thenReturn(span);
        when(tracer.withSpan(any())).thenReturn(mock(Tracer.SpanInScope.class));
    }

    private OutboxEventoJpaEntity pendente(String tipo) {
        var payload = "{\"contratoId\":\"" + contratoId + "\",\"clienteId\":\"" + UUID.randomUUID() + "\"}";
        return new OutboxEventoJpaEntity(UUID.randomUUID(), tipo, contratoId, "corr-1", payload, Instant.now(), null, null);
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<SendResult<String, String>> enviado() {
        return CompletableFuture.completedFuture(mock(SendResult.class));
    }

    @Test
    void publicaComChaveDoContratoEHeadersDeCorrelacao() {
        var evento = pendente("ContratoCriado");
        when(repository.findTop100ByPublicadoEmIsNullOrderByCriadoEmAsc()).thenReturn(List.of(evento));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(enviado());

        publisher.publicarPendentes();

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();

        assertThat(record.topic()).isEqualTo(TOPICO);
        assertThat(record.key()).isEqualTo(contratoId.toString());
        assertThat(record.value()).contains("\"eventType\":\"ContratoCriado\"")
                .contains("\"correlationId\":\"corr-1\"")
                .contains("\"contratoId\":\"" + contratoId + "\"");
        assertThat(header(record, "eventId")).isEqualTo(evento.getEventId().toString());
        assertThat(header(record, "eventType")).isEqualTo("ContratoCriado");
        assertThat(header(record, "contratoId")).isEqualTo(contratoId.toString());
        assertThat(header(record, "correlationId")).isEqualTo("corr-1");
        assertThat(evento.getPublicadoEm()).isNotNull();
    }

    @Test
    void publicaNaOrdemDeCriacaoEMarcaCadaUmComoPublicado() {
        var criado = pendente("ContratoCriado");
        var entrega = pendente("EntregaRegistrada");
        var concluido = pendente("ContratoConcluido");
        when(repository.findTop100ByPublicadoEmIsNullOrderByCriadoEmAsc()).thenReturn(List.of(criado, entrega, concluido));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(enviado());

        publisher.publicarPendentes();

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(3)).send(captor.capture());
        assertThat(captor.getAllValues()).extracting(r -> header(r, "eventType"))
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(List.of(criado, entrega, concluido)).allSatisfy(e -> assertThat(e.getPublicadoEm()).isNotNull());
    }

    @Test
    void falhaNoEnvioInterrompeOCicloParaPreservarAOrdem() {
        var criado = pendente("ContratoCriado");
        var entrega = pendente("EntregaRegistrada");
        when(repository.findTop100ByPublicadoEmIsNullOrderByCriadoEmAsc()).thenReturn(List.of(criado, entrega));
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker indisponível")));

        publisher.publicarPendentes();

        verify(kafkaTemplate, times(1)).send(any(ProducerRecord.class));
        assertThat(criado.getPublicadoEm()).isNull();
        assertThat(criado.getTentativas()).isEqualTo(1);
        assertThat(entrega.getPublicadoEm()).isNull();
        assertThat(entrega.getTentativas()).isZero();
    }

    private static String header(ProducerRecord<String, String> record, String nome) {
        return new String(record.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }
}
