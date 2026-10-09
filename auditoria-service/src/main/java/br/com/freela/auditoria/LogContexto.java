package br.com.freela.auditoria;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import java.nio.charset.StandardCharsets;

/** Chaves de contexto impressas em toda linha de log (ver o pattern no application.yml). */
final class LogContexto {
    static final String CORRELATION_ID = "correlationId";
    static final String CONTRATO_ID = "contratoId";
    static final String EVENT_ID = "eventId";

    private LogContexto() {}

    static void definir(EventoContratoMensagem m) {
        MDC.put(CORRELATION_ID, m.correlationId() == null ? "-" : m.correlationId());
        MDC.put(CONTRATO_ID, String.valueOf(m.contratoId()));
        MDC.put(EVENT_ID, String.valueOf(m.eventId()));
    }

    static void limpar() {
        MDC.remove(CORRELATION_ID);
        MDC.remove(CONTRATO_ID);
        MDC.remove(EVENT_ID);
    }

    /** Lê um header da mensagem; é a fonte dos ids quando o corpo nem pôde ser interpretado. */
    static String header(ConsumerRecord<?, ?> record, String nome) {
        Header h = record.headers().lastHeader(nome);
        return h == null || h.value() == null ? "-" : new String(h.value(), StandardCharsets.UTF_8);
    }
}
