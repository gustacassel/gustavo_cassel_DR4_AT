package br.com.freela.reputacao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import tools.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/** Envelope publicado pelo contrato-service no tópico de eventos. */
@JsonIgnoreProperties(ignoreUnknown = true)
record EventoContratoMensagem(UUID eventId, String eventType, UUID contratoId, Instant occurredAt,
                              String correlationId, JsonNode payload) {
    UUID freelancerId() { return UUID.fromString(payload.path("freelancerId").asText()); }
    BigDecimal valor() { return payload.path("valor").decimalValue(); }
}
