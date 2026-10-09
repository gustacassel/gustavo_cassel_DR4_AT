package br.com.freela.notificacao;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declara os tópicos com o mesmo número de partições do produtor: quem subir primeiro já os cria corretos. */
@Configuration
class KafkaTopicosConfig {
    @Bean
    NewTopic topicoContratosEventos(@Value("${app.kafka.topico-contratos}") String nome,
                                    @Value("${app.kafka.particoes}") int particoes) {
        return TopicBuilder.name(nome).partitions(particoes).replicas(1).build();
    }

    // A DLT tem as mesmas partições do tópico de origem para preservar a partição da mensagem com erro.
    @Bean
    NewTopic topicoContratosEventosDlt(@Value("${app.kafka.topico-contratos-dlt}") String nome,
                                       @Value("${app.kafka.particoes}") int particoes) {
        return TopicBuilder.name(nome).partitions(particoes).replicas(1).build();
    }
}
