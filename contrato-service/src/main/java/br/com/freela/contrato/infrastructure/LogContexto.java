package br.com.freela.contrato.infrastructure;

import org.slf4j.MDC;
import java.util.UUID;

/** Chaves de contexto impressas em toda linha de log (ver o pattern no application.yml). */
public final class LogContexto {
    public static final String CORRELATION_ID = "correlationId";
    public static final String CONTRATO_ID = "contratoId";
    public static final String EVENT_ID = "eventId";

    private LogContexto() {}

    public static void definirContrato(UUID contratoId) {
        MDC.put(CONTRATO_ID, String.valueOf(contratoId));
    }

    public static void definir(String correlationId, UUID contratoId, UUID eventId) {
        MDC.put(CORRELATION_ID, correlationId == null ? "-" : correlationId);
        MDC.put(CONTRATO_ID, String.valueOf(contratoId));
        MDC.put(EVENT_ID, String.valueOf(eventId));
    }

    public static void limparContrato() {
        MDC.remove(CONTRATO_ID);
    }

    public static void limpar() {
        MDC.remove(CORRELATION_ID);
        MDC.remove(CONTRATO_ID);
        MDC.remove(EVENT_ID);
    }
}
