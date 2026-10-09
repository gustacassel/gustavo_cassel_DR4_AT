package br.com.freela.notificacao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** Envelope publicado pelo contrato-service no tópico de eventos. */
@JsonIgnoreProperties(ignoreUnknown = true)
record EventoContratoMensagem(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                              String correlationId, JsonNode payload) {
    UUID clienteId() { return UUID.fromString(payload.path("clienteId").asText()); }
    UUID freelancerId() { return UUID.fromString(payload.path("freelancerId").asText()); }
    String titulo() { return payload.path("titulo").asText(); }
}
