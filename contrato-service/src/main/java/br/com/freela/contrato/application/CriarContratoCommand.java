package br.com.freela.contrato.application;
import java.math.BigDecimal;
import java.util.UUID;
public record CriarContratoCommand(UUID clienteId, UUID freelancerId, String titulo, BigDecimal valor) {}
