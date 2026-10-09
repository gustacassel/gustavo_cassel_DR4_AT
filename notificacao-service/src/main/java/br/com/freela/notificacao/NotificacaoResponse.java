package br.com.freela.notificacao;
import java.time.Instant; import java.util.UUID;
record NotificacaoResponse(UUID id, UUID eventId, UUID contratoId, UUID destinatarioId, String tipo, String mensagem, Instant criadaEm) {
 static NotificacaoResponse from(Notificacao n){ return new NotificacaoResponse(n.id,n.eventId,n.contratoId,n.destinatarioId,n.tipo,n.mensagem,n.criadaEm); }
}
