# Especificação das mensagens

Contrato de comunicação entre os microsserviços do Freela Marketplace. Toda comunicação entre o
`contrato-service` e os serviços auxiliares acontece por estes eventos; não há chamadas HTTP entre eles.

## Canal

| Item | Valor |
|---|---|
| Tópico | `contratos.eventos` |
| Partições | 3 (declarado por todos os serviços) |
| Chave da mensagem | `contratoId` (UUID em texto) — garante a ordem dos eventos de um mesmo contrato |
| Produtor | `contrato-service` (via outbox, `acks=all`, produtor idempotente) |
| Consumidores | `notificacao-service`, `reputacao-service`, `auditoria-service` (um consumer group por serviço) |
| Serialização | JSON em string UTF-8 (`StringSerializer` / `StringDeserializer`) |
| Dead Letter Topic | `contratos.eventos.DLT`, 3 partições, mesma chave e partição da mensagem original |

### Headers Kafka

| Header | Conteúdo |
|---|---|
| `eventId` | id único do evento (igual ao do envelope) |
| `eventType` | tipo do evento |
| `contratoId` | id do contrato |
| `correlationId` | id de correlação da operação que originou o evento |
| `traceparent` | contexto de tracing W3C (Micrometer/Brave), continua o trace nos consumidores |

Os headers repetem os ids do envelope para que, mesmo quando o corpo não possa ser interpretado
(mensagem na DLT), a operação continue identificável nos logs.

## Envelope

Todos os eventos usam o mesmo envelope. `payload` é o evento de domínio tal como emitido pelo Aggregate.

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `eventId` | UUID | sim | identificador único do evento; chave de idempotência nos consumidores |
| `eventType` | string | sim | `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido`, `ContratoCancelado` |
| `contratoId` | UUID | sim | contrato ao qual o evento se refere (também é a chave da mensagem) |
| `occurredAt` | ISO-8601 (UTC) | sim | instante em que o fato ocorreu no domínio |
| `correlationId` | string | não | id de correlação da requisição de origem (vindo do `X-Correlation-Id`) |
| `payload` | objeto | sim | dados do evento (ver abaixo) |

## Eventos

### ContratoCriado

Emitido por `POST /api/contratos`.

| Consumidor | Reação |
|---|---|
| `notificacao-service` | notifica o cliente ("Contrato '…' criado com o freelancer.") e o freelancer ("Você foi contratado para '…'.") |
| `reputacao-service` | ignora (não afeta reputação); marca o evento como processado |
| `auditoria-service` | registra |

Payload — campos obrigatórios: todos.

| Campo | Tipo |
|---|---|
| `eventId`, `occurredAt`, `contratoId` | repetidos do envelope |
| `clienteId` | UUID |
| `freelancerId` | UUID |
| `titulo` | string (até 160) |
| `valor` | decimal > 0 |

Exemplo (chave `808a5c9c-b679-4caf-a7d1-e3191fa048aa`):

```json
{
  "eventId": "4e0d9f4f-0de7-49b4-9496-2f0f5c198a09",
  "eventType": "ContratoCriado",
  "contratoId": "808a5c9c-b679-4caf-a7d1-e3191fa048aa",
  "occurredAt": "2026-10-07T23:56:25.081593Z",
  "correlationId": "teste-fluxo-001",
  "payload": {
    "eventId": "4e0d9f4f-0de7-49b4-9496-2f0f5c198a09",
    "occurredAt": "2026-10-07T23:56:25.081593073Z",
    "contratoId": "808a5c9c-b679-4caf-a7d1-e3191fa048aa",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222",
    "titulo": "API de pagamentos",
    "valor": 3500.0
  }
}
```

### EntregaRegistrada

Emitido por `POST /api/contratos/{id}/entrega` (somente contratos ATIVOS).

| Consumidor | Reação |
|---|---|
| `notificacao-service` | notifica o cliente ("O freelancer registrou a entrega. Revise e conclua o contrato.") |
| `reputacao-service` | ignora; marca como processado |
| `auditoria-service` | registra |

Payload — `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId` (todos obrigatórios).

```json
{
  "eventId": "cbbe29c9-5514-4119-b180-d0ffea530d2b",
  "eventType": "EntregaRegistrada",
  "contratoId": "c4d7d597-4820-40af-b535-581419783b35",
  "occurredAt": "2026-10-08T00:29:01.148863Z",
  "correlationId": "teste-auditoria-004",
  "payload": {
    "eventId": "cbbe29c9-5514-4119-b180-d0ffea530d2b",
    "occurredAt": "2026-10-08T00:29:01.148863Z",
    "contratoId": "c4d7d597-4820-40af-b535-581419783b35",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "22222222-2222-2222-2222-222222222222"
  }
}
```

### ContratoConcluido

Emitido por `POST /api/contratos/{id}/conclusao` (somente após a entrega registrada).

| Consumidor | Reação |
|---|---|
| `notificacao-service` | notifica o freelancer ("O cliente concluiu o contrato.") e o cliente ("Contrato concluído.") |
| `reputacao-service` | `contratosConcluidos + 1` e `valorTotal + valor` do freelancer |
| `auditoria-service` | registra |

Payload — `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId`, `valor` (todos obrigatórios).

```json
{
  "eventId": "fd245ef8-5c16-4d3e-8fd0-9e9c7a1d7d3b",
  "eventType": "ContratoConcluido",
  "contratoId": "ad3db2b3-d9b2-41af-b1a9-6ca19c9f1965",
  "occurredAt": "2026-10-10T21:33:31.812Z",
  "correlationId": "corr-graylog-183325",
  "payload": {
    "eventId": "fd245ef8-5c16-4d3e-8fd0-9e9c7a1d7d3b",
    "occurredAt": "2026-10-10T21:33:31.812Z",
    "contratoId": "ad3db2b3-d9b2-41af-b1a9-6ca19c9f1965",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "99999999-9999-9999-9999-999999999999",
    "valor": 640.0
  }
}
```

### ContratoCancelado

Emitido por `POST /api/contratos/{id}/cancelamento` (contratos não concluídos nem já cancelados).

| Consumidor | Reação |
|---|---|
| `notificacao-service` | notifica cliente e freelancer ("Contrato cancelado.") |
| `reputacao-service` | `contratosCancelados + 1` do freelancer |
| `auditoria-service` | registra |

Payload — `eventId`, `occurredAt`, `contratoId`, `clienteId`, `freelancerId` (todos obrigatórios).

```json
{
  "eventId": "2c1f0a9e-7b7d-4c8a-9f1e-0d3b5a6c7e8f",
  "eventType": "ContratoCancelado",
  "contratoId": "77fcb3ad-073b-4cb8-8244-a0db05da22d8",
  "occurredAt": "2026-10-09T18:48:50.082900Z",
  "correlationId": "teste-etapa5",
  "payload": {
    "eventId": "2c1f0a9e-7b7d-4c8a-9f1e-0d3b5a6c7e8f",
    "occurredAt": "2026-10-09T18:48:50.082900Z",
    "contratoId": "77fcb3ad-073b-4cb8-8244-a0db05da22d8",
    "clienteId": "11111111-1111-1111-1111-111111111111",
    "freelancerId": "44444444-4444-4444-4444-444444444444"
  }
}
```

## Ordem esperada por contrato

Dentro da partição do contrato, as mensagens chegam na ordem de criação na outbox:

```text
ContratoCriado -> EntregaRegistrada -> ContratoConcluido
ContratoCriado -> ContratoCancelado
ContratoCriado -> EntregaRegistrada -> ContratoCancelado
```

Os consumidores não dependem de outro evento para processar um evento (cada um é autossuficiente),
mas a ordem é preservada para que o estado derivado (notificações, reputação, auditoria) reflita a
sequência real dos fatos.

## Mensagens com erro (DLT)

Registros em `contratos.eventos.DLT` são cópias da mensagem original (mesma chave, mesmo valor,
mesmos headers) acrescidas dos headers do Spring Kafka:

| Header | Conteúdo |
|---|---|
| `kafka_dlt-original-topic` / `-partition` / `-offset` / `-timestamp` | origem da mensagem |
| `kafka_dlt-original-consumer-group` | consumidor que desistiu dela |
| `kafka_dlt-exception-fqcn` / `-message` / `-stacktrace` | causa |

Como cada consumidor trata a falha de forma independente, uma mensagem inválida pode aparecer uma
vez por consumidor na DLT.
