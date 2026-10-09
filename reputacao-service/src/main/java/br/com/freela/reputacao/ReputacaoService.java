package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.math.BigDecimal; import java.util.UUID;
@Service
public class ReputacaoService {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class);
 private final ReputacaoRepository repository; private final EventoProcessadoRepository processados;
 public ReputacaoService(ReputacaoRepository r, EventoProcessadoRepository p){this.repository=r;this.processados=p;}

 /** Idempotente: contadores só são alterados uma vez por eventId, na mesma transação em que o evento é marcado como processado. */
 @Transactional boolean processar(EventoContratoMensagem m){
  if(processados.existsById(m.eventId())){
   log.warn("reputacao.evento.duplicado eventId={} contratoId={} eventType={} correlationId={}",m.eventId(),m.contratoId(),m.eventType(),m.correlationId());
   return false;
  }
  switch(m.eventType()){
   case "ContratoConcluido" -> registrarContratoConcluido(m.contratoId(), m.freelancerId(), m.valor());
   case "ContratoCancelado" -> registrarContratoCancelado(m.contratoId(), m.freelancerId());
   default -> log.info("reputacao.evento.ignorado eventId={} contratoId={} eventType={}",m.eventId(),m.contratoId(),m.eventType());
  }
  processados.save(new EventoProcessado(m.eventId(), m.eventType()));
  return true;
 }

 void registrarContratoConcluido(UUID contratoId,UUID freelancerId,BigDecimal valor){
  log.info("reputacao.atualizacao.inicio contratoId={} freelancerId={} valor={}",contratoId,freelancerId,valor);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId)); r.registrarContrato(valor); repository.save(r);
  log.info("reputacao.atualizacao.sucesso contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",contratoId,freelancerId,r.contratosConcluidos,r.valorTotal);
 }

 void registrarContratoCancelado(UUID contratoId,UUID freelancerId){
  log.info("reputacao.cancelamento.inicio contratoId={} freelancerId={}",contratoId,freelancerId);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId)); r.registrarCancelamento(); repository.save(r);
  log.info("reputacao.cancelamento.sucesso contratoId={} freelancerId={} contratosCancelados={}",contratoId,freelancerId,r.contratosCancelados);
 }
}
