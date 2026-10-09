package br.com.freela.reputacao;
import java.math.BigDecimal; import java.util.UUID;
record ReputacaoResponse(UUID freelancerId, int contratosConcluidos, int contratosCancelados, BigDecimal valorTotal) {
 static ReputacaoResponse from(ReputacaoFreelancer r){ return new ReputacaoResponse(r.freelancerId,r.contratosConcluidos,r.contratosCancelados,r.valorTotal); }
}
