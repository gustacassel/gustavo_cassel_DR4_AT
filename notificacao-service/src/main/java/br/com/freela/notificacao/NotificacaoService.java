package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
@Service
public class NotificacaoService {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class);
 private final NotificacaoRepository repository; private final EventoProcessadoRepository processados;
 public NotificacaoService(NotificacaoRepository r, EventoProcessadoRepository p){this.repository=r;this.processados=p;}

 /** Idempotente: a verificação do eventId e a gravação das notificações ocorrem na mesma transação. */
 @Transactional boolean processar(EventoContratoMensagem m){
  if(processados.existsById(m.eventId())){
   log.warn("notificacao.evento.duplicado eventId={} contratoId={} eventType={} correlationId={}",m.eventId(),m.contratoId(),m.eventType(),m.correlationId());
   return false;
  }
  switch(m.eventType()){
   case "ContratoCriado" -> {
    registrar(m, m.clienteId(), "Contrato '"+m.titulo()+"' criado com o freelancer.");
    registrar(m, m.freelancerId(), "Você foi contratado para '"+m.titulo()+"'.");
   }
   case "EntregaRegistrada" -> registrar(m, m.clienteId(), "O freelancer registrou a entrega. Revise e conclua o contrato.");
   case "ContratoConcluido" -> {
    registrar(m, m.freelancerId(), "O cliente concluiu o contrato.");
    registrar(m, m.clienteId(), "Contrato concluído.");
   }
   case "ContratoCancelado" -> {
    registrar(m, m.clienteId(), "Contrato cancelado.");
    registrar(m, m.freelancerId(), "Contrato cancelado.");
   }
   default -> log.info("notificacao.evento.ignorado eventId={} contratoId={} eventType={}",m.eventId(),m.contratoId(),m.eventType());
  }
  processados.save(new EventoProcessado(m.eventId(), m.eventType()));
  return true;
 }

 private void registrar(EventoContratoMensagem m, UUID destinatarioId, String mensagem){
  log.info("notificacao.registro.inicio eventId={} contratoId={} destinatarioId={} tipo={}",m.eventId(),m.contratoId(),destinatarioId,m.eventType());
  var n=repository.save(new Notificacao(m.eventId(),m.contratoId(),destinatarioId,m.eventType(),mensagem));
  log.info("notificacao.registro.sucesso notificacaoId={} eventId={} contratoId={} destinatarioId={}",n.id,m.eventId(),m.contratoId(),destinatarioId);
 }
}
