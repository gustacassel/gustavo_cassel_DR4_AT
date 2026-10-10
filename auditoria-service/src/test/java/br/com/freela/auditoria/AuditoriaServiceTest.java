package br.com.freela.auditoria;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AuditoriaServiceTest {

    private final EventoAuditoriaRepository repository = mock(EventoAuditoriaRepository.class);
    private final AuditoriaService service = new AuditoriaService(repository);

    @Test
    void registraUmEventoNovo() {
        UUID eventId = UUID.randomUUID();
        when(repository.existsByEventId(eventId)).thenReturn(false);
        when(repository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        boolean registrado = service.registrar(eventId, UUID.randomUUID(), "ContratoCriado", "corr-1", "{}");

        assertThat(registrado).isTrue();
        verify(repository).save(any(EventoAuditoria.class));
    }

    @Test
    void eventoJaRegistradoEIgnorado() {
        UUID eventId = UUID.randomUUID();
        when(repository.existsByEventId(eventId)).thenReturn(true);

        boolean registrado = service.registrar(eventId, UUID.randomUUID(), "ContratoCriado", "corr-1", "{}");

        assertThat(registrado).isFalse();
        verify(repository, never()).save(any());
    }
}
