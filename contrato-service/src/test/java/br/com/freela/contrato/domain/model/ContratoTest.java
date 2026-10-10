package br.com.freela.contrato.domain.model;

import br.com.freela.contrato.domain.event.ContratoCancelado;
import br.com.freela.contrato.domain.event.ContratoConcluido;
import br.com.freela.contrato.domain.event.ContratoCriado;
import br.com.freela.contrato.domain.event.EntregaRegistrada;
import br.com.freela.contrato.domain.shared.DomainEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ContratoTest {

    private static Contrato novoContrato() {
        return Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "API de pagamentos", new BigDecimal("3500.00"));
    }

    @Test
    void criarEmiteContratoCriadoComOsDadosDoContrato() {
        Contrato contrato = novoContrato();

        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.domainEvents()).hasSize(1);
        DomainEvent evento = contrato.domainEvents().get(0);
        assertThat(evento).isInstanceOf(ContratoCriado.class);
        assertThat(evento.contratoId()).isEqualTo(contrato.id());
        assertThat(evento.eventType()).isEqualTo("ContratoCriado");
        assertThat(evento.eventId()).isNotNull();
        assertThat(evento.occurredAt()).isNotNull();
    }

    @Test
    void cicloCompletoEmiteOsEventosNaOrdemDasTransicoes() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();
        contrato.concluir();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CONCLUIDO);
        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventType)
                .containsExactly("ContratoCriado", "EntregaRegistrada", "ContratoConcluido");
        assertThat(contrato.domainEvents().get(1)).isInstanceOf(EntregaRegistrada.class);
        assertThat(contrato.domainEvents().get(2)).isInstanceOfSatisfying(ContratoConcluido.class,
                e -> assertThat(e.valor()).isEqualByComparingTo("3500.00"));
    }

    @Test
    void cancelarEmiteContratoCancelado() {
        Contrato contrato = novoContrato();
        contrato.cancelar();

        assertThat(contrato.status()).isEqualTo(StatusContrato.CANCELADO);
        assertThat(contrato.domainEvents().get(1)).isInstanceOf(ContratoCancelado.class);
    }

    @Test
    void cadaEventoTemUmIdDiferente() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();
        contrato.concluir();

        assertThat(contrato.domainEvents()).extracting(DomainEvent::eventId).doesNotHaveDuplicates();
    }

    @Test
    void pullDomainEventsDevolveOsEventosEEsvaziaALista() {
        Contrato contrato = novoContrato();
        contrato.registrarEntrega();

        var eventos = contrato.pullDomainEvents();

        assertThat(eventos).hasSize(2);
        assertThat(contrato.domainEvents()).isEmpty();
    }

    @Test
    void transicoesInvalidasNaoMudamOEstadoNemEmitemEvento() {
        Contrato contrato = novoContrato();

        assertThatThrownBy(contrato::concluir).isInstanceOf(IllegalStateException.class);
        assertThat(contrato.status()).isEqualTo(StatusContrato.ATIVO);
        assertThat(contrato.domainEvents()).hasSize(1);

        contrato.registrarEntrega();
        contrato.concluir();
        assertThatThrownBy(contrato::cancelar).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(contrato::registrarEntrega).isInstanceOf(IllegalStateException.class);
        assertThat(contrato.domainEvents()).hasSize(3);
    }

    @Test
    void naoCriaContratoComValorInvalido() {
        assertThatThrownBy(() -> Contrato.criar(UUID.randomUUID(), UUID.randomUUID(), "x", BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
