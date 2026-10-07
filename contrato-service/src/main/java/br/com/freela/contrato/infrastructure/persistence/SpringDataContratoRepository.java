package br.com.freela.contrato.infrastructure.persistence;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.UUID;
interface SpringDataContratoRepository extends JpaRepository<ContratoJpaEntity, UUID> {}
