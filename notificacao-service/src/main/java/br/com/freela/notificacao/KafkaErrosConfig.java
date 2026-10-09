package br.com.freela.notificacao;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;
import tools.jackson.core.JacksonException;

/**
 * Falha no consumo: N tentativas com intervalo fixo e, esgotadas, a mensagem original vai para
 * o tópico DLT (mesma chave e partição). Falhas sem chance de sucesso pulam o retry.
 */
@Configuration
class KafkaErrosConfig {
    private static final Logger log = LoggerFactory.getLogger(KafkaErrosConfig.class);

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                          @Value("${app.kafka.topico-contratos-dlt}") String topicoDlt,
                                          @Value("${app.kafka.retry.tentativas}") int tentativas,
                                          @Value("${app.kafka.retry.intervalo-ms}") long intervaloMs) {
        // Destino explícito (o sufixo padrão do Spring seria "-dlt"), preservando a partição de origem.
        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition(topicoDlt, record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(intervaloMs, tentativas - 1L));
        handler.addNotRetryableExceptions(JacksonException.class, IllegalArgumentException.class);
        handler.setRetryListeners(new RetryListener() {
            @Override public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int tentativa) {
                log.warn("notificacao.evento.retry topico={} particao={} offset={} key={} tentativa={} erro={}",
                        record.topic(), record.partition(), record.offset(), record.key(), tentativa, ex.getMessage());
            }
            @Override public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("notificacao.evento.dlt topico={} particao={} offset={} key={} erro={}",
                        record.topic(), record.partition(), record.offset(), record.key(), ex.getMessage());
            }
        });
        return handler;
    }
}
