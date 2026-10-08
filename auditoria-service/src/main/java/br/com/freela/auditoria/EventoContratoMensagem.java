package br.com.freela.auditoria;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** Envelope publicado pelo contrato-service no tópico de eventos. */
@JsonIgnoreProperties(ignoreUnknown = true)
record EventoContratoMensagem(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                              String correlationId, JsonNode payload) {
}
