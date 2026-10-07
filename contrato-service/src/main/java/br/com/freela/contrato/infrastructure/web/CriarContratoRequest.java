package br.com.freela.contrato.infrastructure.web;
import jakarta.validation.constraints.*;
import java.math.BigDecimal;
import java.util.UUID;
public record CriarContratoRequest(
        @NotNull UUID clienteId,
        @NotNull UUID freelancerId,
        @NotBlank @Size(max=160) String titulo,
        @NotNull @DecimalMin(value="0.01") BigDecimal valor) {}
