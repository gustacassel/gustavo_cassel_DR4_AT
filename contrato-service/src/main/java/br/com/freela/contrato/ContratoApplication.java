package br.com.freela.contrato;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication
@EnableScheduling
public class ContratoApplication {
    public static void main(String[] args) { SpringApplication.run(ContratoApplication.class, args); }
}
