package br.com.freela.reputacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class ContratoEventosConsumer {
    private static final Logger log = LoggerFactory.getLogger(ContratoEventosConsumer.class);
    private final ReputacaoService service;
    private final ObjectMapper objectMapper;

    public ContratoEventosConsumer(ReputacaoService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${app.kafka.topico-contratos}", groupId = "${spring.kafka.consumer.group-id}",
                   concurrency = "${app.kafka.concurrencia}")
    public void consumir(ConsumerRecord<String, String> record) {
        var mensagem = objectMapper.readValue(record.value(), EventoContratoMensagem.class);
        LogContexto.definir(mensagem);
        try {
            log.info("reputacao.evento.recebido topico={} particao={} offset={} key={} eventType={}",
                    record.topic(), record.partition(), record.offset(), record.key(), mensagem.eventType());

            boolean processado = service.processar(mensagem);

            log.info("reputacao.evento.processado eventType={} resultado={}",
                    mensagem.eventType(), processado ? "PROCESSADO" : "DUPLICADO_IGNORADO");
        } finally {
            LogContexto.limpar();
        }
    }
}
