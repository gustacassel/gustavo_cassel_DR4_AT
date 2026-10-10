package br.com.freela.reputacao;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Consumidor de ponta a ponta com broker embutido: duplicidade (mesmo eventId duas vezes)
 * e ordem dos eventos de um mesmo contrato (mesma chave -> mesma partição).
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:reputacao;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "eureka.client.enabled=false",
        "management.tracing.export.enabled=false"
})
@EmbeddedKafka(partitions = 3, topics = {"contratos.eventos", "contratos.eventos.DLT"})
class ConsumidorKafkaIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ReputacaoRepository reputacoes;
    @Autowired EventoProcessadoRepository processados;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void mensagemDuplicadaNaoIncrementaDuasVezes() throws Exception {
        UUID freelancerId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        publicar(contratoId, eventId, "ContratoConcluido", freelancerId, "300.00");
        publicar(contratoId, eventId, "ContratoConcluido", freelancerId, "300.00");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processados.existsById(eventId)).isTrue());

        ReputacaoFreelancer r = reputacoes.findById(freelancerId).orElseThrow();
        assertThat(r.contratosConcluidos).isEqualTo(1);
        assertThat(r.valorTotal).isEqualByComparingTo("300.00");
    }

    @Test
    void eventosDoMesmoContratoSaoProcessadosNaOrdemDePublicacao() throws Exception {
        UUID freelancerId = UUID.randomUUID();
        UUID contratoId = UUID.randomUUID();
        UUID criado = UUID.randomUUID(), concluido = UUID.randomUUID(), cancelado = UUID.randomUUID();

        publicar(contratoId, criado, "ContratoCriado", freelancerId, "50.00");
        publicar(contratoId, concluido, "ContratoConcluido", freelancerId, "50.00");
        publicar(contratoId, cancelado, "ContratoCancelado", freelancerId, "50.00");

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(processados.existsById(cancelado)).isTrue());

        Instant t1 = processados.findById(criado).orElseThrow().processadoEm;
        Instant t2 = processados.findById(concluido).orElseThrow().processadoEm;
        Instant t3 = processados.findById(cancelado).orElseThrow().processadoEm;
        assertThat(t1).isBeforeOrEqualTo(t2);
        assertThat(t2).isBeforeOrEqualTo(t3);

        ReputacaoFreelancer r = reputacoes.findById(freelancerId).orElseThrow();
        assertThat(r.contratosConcluidos).isEqualTo(1);
        assertThat(r.contratosCancelados).isEqualTo(1);
    }

    private void publicar(UUID contratoId, UUID eventId, String tipo, UUID freelancerId, String valor) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("contratoId", contratoId.toString());
        payload.put("clienteId", UUID.randomUUID().toString());
        payload.put("freelancerId", freelancerId.toString());
        payload.put("valor", Double.parseDouble(valor));

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", eventId.toString());
        envelope.put("eventType", tipo);
        envelope.put("contratoId", contratoId.toString());
        envelope.put("occurredAt", Instant.now().toString());
        envelope.put("correlationId", "teste-integracao");
        envelope.put("payload", payload);

        var record = new ProducerRecord<>("contratos.eventos", contratoId.toString(), objectMapper.writeValueAsString(envelope));
        record.headers().add(new RecordHeader("eventId", eventId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("eventType", tipo.getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("contratoId", contratoId.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader("correlationId", "teste-integracao".getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record).get();
    }
}
