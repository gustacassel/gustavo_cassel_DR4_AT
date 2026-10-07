package br.com.freela.contrato.infrastructure.persistence;

import br.com.freela.contrato.domain.model.StatusContrato;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "contratos")
public class ContratoJpaEntity {
    @Id private UUID id;
    @Column(nullable=false) private UUID clienteId;
    @Column(nullable=false) private UUID freelancerId;
    @Column(nullable=false, length=160) private String titulo;
    @Column(nullable=false, precision=19, scale=2) private BigDecimal valor;
    @Enumerated(EnumType.STRING) @Column(nullable=false, length=40) private StatusContrato status;
    @Column(nullable=false) private Instant criadoEm;
    protected ContratoJpaEntity() {}
    public ContratoJpaEntity(UUID id, UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor, StatusContrato status, Instant criadoEm) {
        this.id=id; this.clienteId=clienteId; this.freelancerId=freelancerId; this.titulo=titulo; this.valor=valor; this.status=status; this.criadoEm=criadoEm;
    }
    public UUID getId(){return id;} public UUID getClienteId(){return clienteId;} public UUID getFreelancerId(){return freelancerId;}
    public String getTitulo(){return titulo;} public BigDecimal getValor(){return valor;} public StatusContrato getStatus(){return status;} public Instant getCriadoEm(){return criadoEm;}
}
