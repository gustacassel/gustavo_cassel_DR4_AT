# Pontos deixados para os alunos

## Comunicação por mensagens
- Definir tópicos e contratos de eventos.
- Implementar producer no `contrato-service`.
- Implementar consumidores nos serviços interessados.
- Explicar por que o fluxo é assíncrono.

## Concorrência e ordenação
- Definir a key da mensagem para manter eventos do mesmo `contratoId` na mesma partição.
- Configurar concorrência de consumidores sem quebrar a ordem dentro da partição.

## Duplicidade
- Usar `eventId` como identificador idempotente.
- Demonstrar que uma mesma mensagem processada novamente não altera o resultado duas vezes.

## Mensagens transacionais
- Implementar Outbox no `contrato-service`.
- Garantir atomicidade entre persistência do Aggregate e registro do evento para publicação.

## Observabilidade
- Propagar correlation/trace context pelas mensagens.
- Centralizar logs dos microsserviços.
- Adicionar tracing distribuído e Zipkin.
- Demonstrar um fluxo completo a partir de um contrato.

## Testes
- Testar producer/consumer.
- Testar duplicidade.
- Testar ordenação.
- Testar integração com broker usando estratégia adequada para testes.
