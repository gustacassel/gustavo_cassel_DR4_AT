package br.com.freela.notificacao;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declara o tópico com o mesmo número de partições do produtor: quem subir primeiro já o cria correto. */
@Configuration
class KafkaTopicosConfig {
    @Bean
    NewTopic topicoContratosEventos(@Value("${app.kafka.topico-contratos}") String nome,
                                    @Value("${app.kafka.particoes}") int particoes) {
        return TopicBuilder.name(nome).partitions(particoes).replicas(1).build();
    }
}
