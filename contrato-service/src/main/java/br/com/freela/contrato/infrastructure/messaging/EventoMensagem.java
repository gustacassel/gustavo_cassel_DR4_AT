package br.com.freela.contrato.infrastructure.messaging;

import tools.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/** Envelope publicado no Kafka. É o contrato de comunicação com os demais serviços. */
public record EventoMensagem(
        UUID eventId,
        String eventType,
        UUID contratoId,
        Instant occurredAt,
        String correlationId,
        JsonNode payload) {
}
