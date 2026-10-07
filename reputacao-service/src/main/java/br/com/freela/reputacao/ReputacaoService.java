package br.com.freela.reputacao;
import org.slf4j.*; import org.springframework.stereotype.Service; import org.springframework.transaction.annotation.Transactional; import java.math.BigDecimal; import java.util.UUID;
@Service
public class ReputacaoService {
 private static final Logger log=LoggerFactory.getLogger(ReputacaoService.class); private final ReputacaoRepository repository;
 public ReputacaoService(ReputacaoRepository r){this.repository=r;}
 @Transactional public void registrarContratoConcluido(UUID contratoId,UUID freelancerId,BigDecimal valor){
  log.info("reputacao.atualizacao.inicio contratoId={} freelancerId={} valor={}",contratoId,freelancerId,valor);
  var r=repository.findById(freelancerId).orElseGet(()->new ReputacaoFreelancer(freelancerId)); r.registrarContrato(valor); repository.save(r);
  log.info("reputacao.atualizacao.sucesso contratoId={} freelancerId={} contratosConcluidos={} valorTotal={}",contratoId,freelancerId,r.contratosConcluidos,r.valorTotal);
 }
}
