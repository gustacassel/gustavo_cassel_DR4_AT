package br.com.freela.auditoria;
import java.time.Instant; import java.util.UUID;
record EventoAuditoriaResponse(UUID id, UUID eventId, UUID aggregateId, String eventType, String correlationId, String payload, Instant recebidoEm) {
 static EventoAuditoriaResponse from(EventoAuditoria e){ return new EventoAuditoriaResponse(e.id,e.eventId,e.aggregateId,e.eventType,e.correlationId,e.payload,e.recebidoEm); }
}
