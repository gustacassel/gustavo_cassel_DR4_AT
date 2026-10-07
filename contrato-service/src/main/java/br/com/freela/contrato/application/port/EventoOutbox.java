package br.com.freela.contrato.application.port;

import br.com.freela.contrato.domain.shared.DomainEvent;

/** Registra um evento de domínio para publicação posterior, na mesma transação do Aggregate. */
public interface EventoOutbox {
    void registrar(DomainEvent evento);
}
