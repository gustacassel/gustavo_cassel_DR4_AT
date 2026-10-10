package br.com.freela.contrato;

import br.com.freela.contrato.application.ContratoApplicationService;
import br.com.freela.contrato.application.CriarContratoCommand;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.infrastructure.outbox.SpringDataOutboxRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Produtor de ponta a ponta: caso de uso -> outbox -> Kafka (broker embutido, 3 partições).
 * Verifica chave, ordem e marcação de publicado.
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:contrato;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "eureka.client.enabled=false",
        "management.tracing.export.enabled=false",
        "app.outbox.intervalo-ms=200"
})
@EmbeddedKafka(partitions = 3, topics = "contratos.eventos")
class OutboxKafkaIntegrationTest {

    @Autowired ContratoApplicationService service;
    @Autowired SpringDataOutboxRepository outbox;
    @Autowired EmbeddedKafkaBroker broker;

    @Test
    void eventosDoContratoChegamAoKafkaNaMesmaParticaoENaOrdemDasTransicoes() {
        Contrato contrato = service.criar(new CriarContratoCommand(
                UUID.randomUUID(), UUID.randomUUID(), "Integração outbox", new BigDecimal("1200.00")));
        service.registrarEntrega(contrato.id());
        service.concluir(contrato.id());

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(outbox.findTop100ByPublicadoEmIsNullOrderByCriadoEmAsc()).isEmpty());

        List<ConsumerRecord<String, String>> doContrato = new ArrayList<>();
        try (Consumer<String, String> consumer = consumidorDeTeste()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "contratos.eventos");
            for (ConsumerRecord<String, String> record : KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10))) {
                if (contrato.id().toString().equals(record.key())) doContrato.add(record);
            }
        }

        assertThat(doContrato).hasSize(3);
        assertThat(doContrato).extracting(r -> header(r, "eventType"))
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(doContrato).extracting(ConsumerRecord::partition).containsOnly(doContrato.get(0).partition());
        assertThat(doContrato).extracting(ConsumerRecord::offset).isSorted();
        assertThat(doContrato.get(0).value()).contains("\"eventType\":\"ContratoCriado\"")
                .contains("\"contratoId\":\"" + contrato.id() + "\"");
    }

    private Consumer<String, String> consumidorDeTeste() {
        var props = KafkaTestUtils.consumerProps(broker, "teste-outbox-" + UUID.randomUUID(), false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer()).createConsumer();
    }

    private static String header(ConsumerRecord<String, String> record, String nome) {
        return new String(record.headers().lastHeader(nome).value(), StandardCharsets.UTF_8);
    }
}
