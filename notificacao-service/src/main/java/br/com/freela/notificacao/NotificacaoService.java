package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.util.UUID;
@Service
public class NotificacaoService {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoService.class); private final NotificacaoRepository repository;
 public NotificacaoService(NotificacaoRepository r){this.repository=r;}
 @Transactional public void registrar(UUID contratoId, UUID destinatarioId, String tipo, String mensagem){
  log.info("notificacao.registro.inicio contratoId={} destinatarioId={} tipo={}",contratoId,destinatarioId,tipo);
  var n=repository.save(new Notificacao(contratoId,destinatarioId,tipo,mensagem));
  log.info("notificacao.registro.sucesso notificacaoId={} contratoId={} destinatarioId={}",n.id,contratoId,destinatarioId);
 }
}
