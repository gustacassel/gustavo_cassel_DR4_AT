package br.com.freela.contrato.infrastructure.outbox;

import br.com.freela.contrato.application.port.EventoOutbox;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.web.CorrelationIdFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.ObjectMapper;

@Component
public class OutboxJpaAdapter implements EventoOutbox {
    private static final Logger log = LoggerFactory.getLogger(OutboxJpaAdapter.class);
    private final SpringDataOutboxRepository repository;
    private final ObjectMapper objectMapper;

    public OutboxJpaAdapter(SpringDataOutboxRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    // MANDATORY: o evento só pode ser registrado dentro da transação que persistiu o Aggregate.
    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void registrar(DomainEvent evento) {
        String correlationId = MDC.get(CorrelationIdFilter.MDC_KEY);
        String payload = objectMapper.writeValueAsString(evento);
        repository.save(new OutboxEventoJpaEntity(evento.eventId(), evento.eventType(), evento.contratoId(),
                correlationId, payload, evento.occurredAt()));
        log.info("contrato.outbox.registrado contratoId={} eventId={} eventType={} correlationId={}",
                evento.contratoId(), evento.eventId(), evento.eventType(), correlationId);
    }
}
