package br.com.freela.reputacao;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ReputacaoServiceTest {

    private final ReputacaoRepository reputacoes = mock(ReputacaoRepository.class);
    private final EventoProcessadoRepository processados = mock(EventoProcessadoRepository.class);
    private final ReputacaoService service = new ReputacaoService(reputacoes, processados);

    private final UUID freelancerId = UUID.randomUUID();
    private final ReputacaoFreelancer reputacao = new ReputacaoFreelancer(freelancerId);

    private EventoContratoMensagem mensagem(UUID eventId, String tipo, String valor) {
        var payload = new ObjectMapper().readTree("{\"freelancerId\":\"" + freelancerId + "\",\"valor\":" + valor + "}");
        return new EventoContratoMensagem(eventId, tipo, UUID.randomUUID(), Instant.now(), "corr-1", payload);
    }

    @Test
    void contratoConcluidoIncrementaContadorEValorTotal() {
        when(reputacoes.findById(freelancerId)).thenReturn(Optional.of(reputacao));
        when(processados.existsById(any())).thenReturn(false);

        boolean processado = service.processar(mensagem(UUID.randomUUID(), "ContratoConcluido", "250.50"));

        assertThat(processado).isTrue();
        assertThat(reputacao.contratosConcluidos).isEqualTo(1);
        assertThat(reputacao.valorTotal).isEqualByComparingTo("250.50");
        verify(processados).save(any(EventoProcessado.class));
    }

    @Test
    void mesmoEventoEntregueDuasVezesIncrementaUmaVezSo() {
        UUID eventId = UUID.randomUUID();
        when(reputacoes.findById(freelancerId)).thenReturn(Optional.of(reputacao));
        when(processados.existsById(eventId)).thenReturn(false).thenReturn(true);

        boolean primeira = service.processar(mensagem(eventId, "ContratoConcluido", "100"));
        boolean segunda = service.processar(mensagem(eventId, "ContratoConcluido", "100"));

        assertThat(primeira).isTrue();
        assertThat(segunda).isFalse();
        assertThat(reputacao.contratosConcluidos).isEqualTo(1);
        assertThat(reputacao.valorTotal).isEqualByComparingTo("100");
        verify(reputacoes, times(1)).save(reputacao);
        verify(processados, times(1)).save(any(EventoProcessado.class));
    }

    @Test
    void contratoCanceladoIncrementaSomenteOContadorDeCancelados() {
        when(reputacoes.findById(freelancerId)).thenReturn(Optional.of(reputacao));
        when(processados.existsById(any())).thenReturn(false);

        service.processar(mensagem(UUID.randomUUID(), "ContratoCancelado", "0"));

        assertThat(reputacao.contratosCancelados).isEqualTo(1);
        assertThat(reputacao.contratosConcluidos).isZero();
        assertThat(reputacao.valorTotal).isEqualByComparingTo("0");
    }

    @Test
    void eventosSemImpactoSaoIgnoradosMasMarcadosComoProcessados() {
        when(processados.existsById(any())).thenReturn(false);

        boolean processado = service.processar(mensagem(UUID.randomUUID(), "ContratoCriado", "0"));

        assertThat(processado).isTrue();
        verify(reputacoes, never()).save(any());
        verify(processados).save(any(EventoProcessado.class));
    }
}
