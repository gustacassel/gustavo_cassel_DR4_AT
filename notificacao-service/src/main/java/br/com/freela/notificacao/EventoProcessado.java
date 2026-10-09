package br.com.freela.notificacao;
import jakarta.persistence.*; import java.time.Instant; import java.util.UUID;
/** Marca os eventos já consumidos; o eventId é a chave de idempotência. */
@Entity @Table(name="eventos_processados")
class EventoProcessado {
 @Id UUID eventId; @Column(nullable=false, length=60) String eventType; @Column(nullable=false) Instant processadoEm;
 protected EventoProcessado(){} EventoProcessado(UUID eventId, String eventType){this.eventId=eventId;this.eventType=eventType;this.processadoEm=Instant.now();}
}
