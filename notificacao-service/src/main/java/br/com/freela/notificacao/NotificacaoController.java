package br.com.freela.notificacao;
import org.slf4j.*; import org.springframework.web.bind.annotation.*; import java.util.*;
@RestController @RequestMapping("/api/notificacoes")
public class NotificacaoController {
 private static final Logger log=LoggerFactory.getLogger(NotificacaoController.class); private final NotificacaoRepository repository;
 NotificacaoController(NotificacaoRepository r){this.repository=r;}
 @GetMapping public List<Notificacao> listar(@RequestHeader(value="X-Correlation-Id",required=false) String correlationId){log.info("http.notificacao.listar correlationId={}",correlationId);return repository.findAll();}
}
