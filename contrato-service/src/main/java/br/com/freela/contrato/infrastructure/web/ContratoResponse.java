package br.com.freela.contrato.infrastructure.web;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.model.StatusContrato;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
public record ContratoResponse(UUID id, UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, StatusContrato status, Instant criadoEm) {
    static ContratoResponse from(Contrato c){ return new ContratoResponse(c.id(),c.clienteId(),c.freelancerId(),c.titulo(),c.valor(),c.status(),c.criadoEm()); }
}
