package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.contrato.infrastructure.LogContexto;
import br.com.freela.contrato.infrastructure.messaging.EventoMensagem;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final SpringDataOutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topico;

    public OutboxPublisher(SpringDataOutboxRepository repository, KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper, @Value("${app.kafka.topico-contratos}") String topico) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topico = topico;
    }

    @Scheduled(fixedDelayString = "${app.outbox.intervalo-ms}")
    @Transactional
    public void publicarPendentes() {
        var pendentes = repository.findTop100ByPublicadoEmIsNullOrderByCriadoEmAsc();
        for (var evento : pendentes) {
            LogContexto.definir(evento.getCorrelationId(), evento.getContratoId(), evento.getEventId());
            try {
                kafkaTemplate.send(montarRecord(evento)).get(10, TimeUnit.SECONDS);
                evento.marcarPublicado();
                log.info("contrato.outbox.publicado topico={} contratoId={} eventId={} eventType={} correlationId={}",
                        topico, evento.getContratoId(), evento.getEventId(), evento.getEventType(), evento.getCorrelationId());
            } catch (Exception ex) {
                evento.registrarFalha(ex.getMessage());
                log.error("contrato.outbox.falha contratoId={} eventId={} eventType={} tentativas={}",
                        evento.getContratoId(), evento.getEventId(), evento.getEventType(), evento.getTentativas(), ex);
                // Interrompe o ciclo: pular este evento quebraria a ordem dos eventos do mesmo contrato.
                return;
            } finally {
                LogContexto.limpar();
            }
        }
    }

    private ProducerRecord<String, String> montarRecord(OutboxEventoJpaEntity evento) {
        var mensagem = new EventoMensagem(evento.getEventId(), evento.getEventType(), evento.getContratoId(),
                evento.getOccurredAt(), evento.getCorrelationId(), objectMapper.readTree(evento.getPayload()));
        // Chave = contratoId: eventos do mesmo contrato caem na mesma partição e mantêm a ordem.
        var record = new ProducerRecord<>(topico, evento.getContratoId().toString(), objectMapper.writeValueAsString(mensagem));
        record.headers()
                .add(header("eventId", evento.getEventId().toString()))
                .add(header("eventType", evento.getEventType()))
                .add(header("contratoId", evento.getContratoId().toString()))
                .add(header("correlationId", evento.getCorrelationId()));
        return record;
    }

    private static RecordHeader header(String nome, String valor) {
        return new RecordHeader(nome, valor == null ? new byte[0] : valor.getBytes(StandardCharsets.UTF_8));
    }
}
