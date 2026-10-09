package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ContratoEventosConsumer {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosConsumer.class);
    private final NotificacaoService service;
    private final ObjectMapper objectMapper;

    public ContratoEventosConsumer(NotificacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.topico-contratos}", groupId = "${spring.kafka.consumer.group-id}",
                   concurrency = "${app.kafka.concurrencia}")
    public void consumir(ConsumerRecord<String, String> record) {
        var mensagem = objectMapper.readValue(record.value(), EventoContratoMensagem.class);
        log.info("notificacao.evento.recebido topico={} particao={} offset={} key={} eventId={} eventType={} contratoId={} correlationId={}",
                record.topic(), record.partition(), record.offset(), record.key(), mensagem.eventId(),
                mensagem.eventType(), mensagem.contratoId(), mensagem.correlationId());

        boolean processado = service.processar(mensagem);

        log.info("notificacao.evento.processado eventId={} contratoId={} resultado={}",
                mensagem.eventId(), mensagem.contratoId(), processado ? "PROCESSADO" : "DUPLICADO_IGNORADO");
    }
}
