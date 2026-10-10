package br.com.freela.notificacao;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NotificacaoServiceTest {

    private final NotificacaoRepository notificacoes = mock(NotificacaoRepository.class);
    private final EventoProcessadoRepository processados = mock(EventoProcessadoRepository.class);
    private final NotificacaoService service = new NotificacaoService(notificacoes, processados);

    private final UUID clienteId = UUID.randomUUID();
    private final UUID freelancerId = UUID.randomUUID();
    private final UUID contratoId = UUID.randomUUID();

    private EventoContratoMensagem mensagem(UUID eventId, String tipo) {
        var payload = new ObjectMapper().readTree("{\"clienteId\":\"" + clienteId + "\",\"freelancerId\":\""
                + freelancerId + "\",\"titulo\":\"Site\"}");
        return new EventoContratoMensagem(eventId, tipo, contratoId, Instant.now(), "corr-1", payload);
    }

    @Test
    void contratoCriadoNotificaClienteEFreelancer() {
        when(processados.existsById(any())).thenReturn(false);
        when(notificacoes.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.processar(mensagem(UUID.randomUUID(), "ContratoCriado"));

        ArgumentCaptor<Notificacao> captor = ArgumentCaptor.forClass(Notificacao.class);
        verify(notificacoes, times(2)).save(captor.capture());
        assertThat(captor.getAllValues()).extracting(n -> n.destinatarioId).containsExactly(clienteId, freelancerId);
        assertThat(captor.getAllValues()).allSatisfy(n -> {
            assertThat(n.contratoId).isEqualTo(contratoId);
            assertThat(n.tipo).isEqualTo("ContratoCriado");
            assertThat(n.eventId).isNotNull();
        });
    }

    @Test
    void entregaRegistradaNotificaSomenteOCliente() {
        when(processados.existsById(any())).thenReturn(false);
        when(notificacoes.save(any())).thenAnswer(inv -> inv.getArgument(0));

        service.processar(mensagem(UUID.randomUUID(), "EntregaRegistrada"));

        ArgumentCaptor<Notificacao> captor = ArgumentCaptor.forClass(Notificacao.class);
        verify(notificacoes, times(1)).save(captor.capture());
        assertThat(captor.getValue().destinatarioId).isEqualTo(clienteId);
    }

    @Test
    void mensagemDuplicadaNaoGeraNotificacoesRepetidas() {
        UUID eventId = UUID.randomUUID();
        when(processados.existsById(eventId)).thenReturn(false).thenReturn(true);
        when(notificacoes.save(any())).thenAnswer(inv -> inv.getArgument(0));

        boolean primeira = service.processar(mensagem(eventId, "ContratoConcluido"));
        boolean segunda = service.processar(mensagem(eventId, "ContratoConcluido"));

        assertThat(primeira).isTrue();
        assertThat(segunda).isFalse();
        verify(notificacoes, times(2)).save(any());
        verify(processados, times(1)).save(any(EventoProcessado.class));
    }
}
