package br.com.freela.contrato.application;

import br.com.freela.contrato.application.port.EventoOutbox;
import br.com.freela.contrato.domain.model.Contrato;
import br.com.freela.contrato.domain.repository.ContratoRepository;
import br.com.freela.contrato.domain.shared.DomainEvent;
import br.com.freela.contrato.infrastructure.LogContexto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

@Service
public class ContratoApplicationService {
    private static final Logger log = LoggerFactory.getLogger(ContratoApplicationService.class);
    private final ContratoRepository repository;
    private final EventoOutbox outbox;

    public ContratoApplicationService(ContratoRepository repository, EventoOutbox outbox) {
        this.repository = repository;
        this.outbox = outbox;
    }

    @Transactional
    public Contrato criar(CriarContratoCommand cmd) {
        log.info("contrato.criacao.inicio clienteId={} freelancerId={} titulo={} valor={}",
                cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        Contrato contrato = Contrato.criar(cmd.clienteId(), cmd.freelancerId(), cmd.titulo(), cmd.valor());
        LogContexto.definirContrato(contrato.id());
        try {
            log.info("contrato.dominio.criado contratoId={} status={} domainEvents={}",
                    contrato.id(), contrato.status(), contrato.domainEvents().size());
            Contrato salvo = repository.salvar(contrato);
            registrarEventosPendentes(contrato);
            log.info("contrato.criacao.sucesso contratoId={} clienteId={} freelancerId={} status={}",
                    salvo.id(), salvo.clienteId(), salvo.freelancerId(), salvo.status());
            return salvo;
        } finally {
            LogContexto.limparContrato();
        }
    }

    @Transactional
    public Contrato registrarEntrega(UUID id) {
        return comContrato(id, () -> {
            log.info("contrato.entrega.inicio contratoId={}", id);
            Contrato contrato = carregar(id);
            contrato.registrarEntrega();
            Contrato salvo = repository.salvar(contrato);
            registrarEventosPendentes(contrato);
            log.info("contrato.entrega.sucesso contratoId={} status={}", salvo.id(), salvo.status());
            return salvo;
        });
    }

    @Transactional
    public Contrato concluir(UUID id) {
        return comContrato(id, () -> {
            log.info("contrato.conclusao.inicio contratoId={}", id);
            Contrato contrato = carregar(id);
            contrato.concluir();
            Contrato salvo = repository.salvar(contrato);
            registrarEventosPendentes(contrato);
            log.info("contrato.conclusao.sucesso contratoId={} status={}", salvo.id(), salvo.status());
            return salvo;
        });
    }

    @Transactional
    public Contrato cancelar(UUID id) {
        return comContrato(id, () -> {
            log.info("contrato.cancelamento.inicio contratoId={}", id);
            Contrato contrato = carregar(id);
            contrato.cancelar();
            Contrato salvo = repository.salvar(contrato);
            registrarEventosPendentes(contrato);
            log.info("contrato.cancelamento.sucesso contratoId={} status={}", salvo.id(), salvo.status());
            return salvo;
        });
    }

    @Transactional(readOnly = true)
    public Contrato buscar(UUID id) {
        return comContrato(id, () -> {
            log.info("contrato.busca.inicio contratoId={}", id);
            var contrato = carregar(id);
            log.info("contrato.busca.sucesso contratoId={} status={}", id, contrato.status());
            return contrato;
        });
    }

    private Contrato comContrato(UUID id, Supplier<Contrato> acao) {
        LogContexto.definirContrato(id);
        try {
            return acao.get();
        } finally {
            LogContexto.limparContrato();
        }
    }

    private Contrato carregar(UUID id) {
        return repository.buscarPorId(id).orElseThrow(() -> new ContratoNaoEncontradoException(id));
    }

    private void registrarEventosPendentes(Contrato contrato) {
        for (DomainEvent event : contrato.pullDomainEvents()) {
            log.info("contrato.evento.registrado contratoId={} eventId={} eventType={} occurredAt={}",
                    contrato.id(), event.eventId(), event.eventType(), event.occurredAt());
            outbox.registrar(event);
        }
    }

    @Transactional(readOnly = true)
    public List<Contrato> listar() {
        log.info("contrato.listagem.inicio");
        var contratos = repository.listar();
        log.info("contrato.listagem.sucesso quantidade={}", contratos.size());
        return contratos;
    }
}
