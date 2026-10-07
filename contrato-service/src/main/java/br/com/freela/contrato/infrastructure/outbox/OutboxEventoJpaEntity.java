package br.com.freela.contrato.infrastructure.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_eventos")
public class OutboxEventoJpaEntity {
    @Id private UUID id;
    @Column(nullable = false, unique = true) private UUID eventId;
    @Column(nullable = false, length = 60) private String eventType;
    @Column(nullable = false) private UUID contratoId;
    @Column(length = 80) private String correlationId;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Column(nullable = false) private Instant occurredAt;
    @Column(nullable = false) private Instant criadoEm;
    private Instant publicadoEm;
    private int tentativas;
    @Column(length = 500) private String ultimoErro;

    protected OutboxEventoJpaEntity() {}

    public OutboxEventoJpaEntity(UUID eventId, String eventType, UUID contratoId, String correlationId,
                                 String payload, Instant occurredAt) {
        this.id = UUID.randomUUID();
        this.eventId = eventId;
        this.eventType = eventType;
        this.contratoId = contratoId;
        this.correlationId = correlationId;
        this.payload = payload;
        this.occurredAt = occurredAt;
        this.criadoEm = Instant.now();
    }

    void marcarPublicado() {
        this.publicadoEm = Instant.now();
        this.ultimoErro = null;
    }

    void registrarFalha(String erro) {
        this.tentativas++;
        this.ultimoErro = erro != null && erro.length() > 500 ? erro.substring(0, 500) : erro;
    }

    public UUID getId() { return id; }
    public UUID getEventId() { return eventId; }
    public String getEventType() { return eventType; }
    public UUID getContratoId() { return contratoId; }
    public String getCorrelationId() { return correlationId; }
    public String getPayload() { return payload; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getCriadoEm() { return criadoEm; }
    public Instant getPublicadoEm() { return publicadoEm; }
    public int getTentativas() { return tentativas; }
}
