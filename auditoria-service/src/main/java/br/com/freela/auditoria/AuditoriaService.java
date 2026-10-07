package br.com.freela.auditoria;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
@Service
public class AuditoriaService {
 private static final Logger log=LoggerFactory.getLogger(AuditoriaService.class); private final EventoAuditoriaRepository repository;
 public AuditoriaService(EventoAuditoriaRepository r){this.repository=r;}
 @Transactional public void registrar(UUID eventId,UUID aggregateId,String eventType,String correlationId,String payload){
  log.info("auditoria.registro.inicio eventId={} aggregateId={} eventType={} correlationId={}",eventId,aggregateId,eventType,correlationId);
  var e=repository.save(new EventoAuditoria(eventId,aggregateId,eventType,correlationId,payload));
  log.info("auditoria.registro.sucesso auditoriaId={} eventId={} aggregateId={} eventType={}",e.id,eventId,aggregateId,eventType);
 }
}
