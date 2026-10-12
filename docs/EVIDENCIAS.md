# Evidências de execução

Roteiro para demonstrar os fluxos implementados, com o comando que gera cada evidência e o que se
espera ver. As capturas de tela ficam em `docs/evidencias/`.

Pré-requisito: plataforma no ar (ver README, "Como executar") e Gateway já roteando (~30 s após subir).

Nos exemplos, `$CID` é um `X-Correlation-Id` escolhido para a operação (ex.: `demo-001`) e `$ID` o
id do contrato retornado na criação. Os comandos Kafka rodam dentro do container
(`docker exec freela-kafka /opt/kafka/bin/<comando>`); no Git Bash use `MSYS_NO_PATHCONV=1`.

## 1. Requisição recebida pelo API Gateway

```bash
curl -i -X POST http://localhost:8080/api/contratos -H 'Content-Type: application/json' -H "X-Correlation-Id: $CID" \
  -d '{"clienteId":"11111111-1111-1111-1111-111111111111","freelancerId":"22222222-2222-2222-2222-222222222222","titulo":"Evidência","valor":1200}'
docker logs freela-gateway 2>&1 | grep "correlationId=$CID"
```

Esperado: `201 Created`; linhas `gateway.request.inicio` / `gateway.request.fim` com `traceId` e o
`correlationId` enviado.

## 2. Alteração persistida no contrato-service

```bash
curl -X POST http://localhost:8080/api/contratos/$ID/entrega   -H "X-Correlation-Id: $CID"
curl -X POST http://localhost:8080/api/contratos/$ID/conclusao -H "X-Correlation-Id: $CID"
curl http://localhost:8080/api/contratos/$ID
docker exec freela-postgres psql -U freela -d contrato_db -c "select id, status from contratos where id='$ID'"
docker exec freela-postgres psql -U freela -d contrato_db -c "select event_type, publicado_em, tentativas from outbox_eventos where contrato_id='$ID' order by criado_em"
```

Esperado: `status=CONCLUIDO`; três linhas na outbox, todas com `publicado_em` preenchido.

## 3. Evento publicado no Kafka

```bash
docker exec freela-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic contratos.eventos
docker exec freela-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic contratos.eventos --from-beginning --timeout-ms 5000 \
  --formatter-property print.partition=true --formatter-property print.offset=true --formatter-property print.key=true
```

Esperado: tópico com 3 partições; as mensagens do contrato com chave `$ID`, **na mesma partição e
com offsets crescentes**, na ordem `ContratoCriado`, `EntregaRegistrada`, `ContratoConcluido`.
Alternativa visual: Kafka UI em http://localhost:8090 → Topics → `contratos.eventos` → Messages.

## 4. Consumo pelos serviços interessados

```bash
for s in notificacao reputacao auditoria; do docker logs freela-$s 2>&1 | grep "correlationId=$CID" | grep -E "evento\.(recebido|processado)"; done
docker exec freela-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups
```

Esperado: `evento.recebido` e `evento.processado resultado=PROCESSADO|REGISTRADO` nos três
serviços, com o mesmo `eventId` por mensagem; três consumer groups com `LAG 0`.

## 5. Persistência realizada pelos consumidores

```bash
curl http://localhost:8080/api/notificacoes
curl http://localhost:8080/api/reputacoes
curl http://localhost:8080/api/auditoria
```

Esperado: 5 notificações do contrato (2 na criação, 1 na entrega, 2 na conclusão) com `eventId`;
freelancer com `contratosConcluidos=1` e `valorTotal` = valor do contrato; 3 registros de auditoria
com `eventId`, `aggregateId`, `eventType`, `correlationId` e `payload`.

## 6. Tratamento de mensagem duplicada

Reentrega todas as mensagens aos três consumidores e mostra que nada muda.

```bash
cd infra
CF="-f docker-compose.yml -f docker-compose.apps.yml -f docker-compose.observabilidade.yml"
docker compose $CF stop notificacao-service reputacao-service auditoria-service
for g in notificacao-service reputacao-service auditoria-service; do
  docker exec freela-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group $g --topic contratos.eventos --reset-offsets --to-earliest --execute
done
docker compose $CF start notificacao-service reputacao-service auditoria-service
# após ~15 s
for s in notificacao reputacao auditoria; do docker logs --since 2m freela-$s 2>&1 | grep -c DUPLICADO_IGNORADO; done
curl http://localhost:8080/api/reputacoes
```

Esperado: cada consumidor loga `evento.duplicado` / `DUPLICADO_IGNORADO` para cada mensagem
reentregue; contagem de notificações, registros de auditoria e os contadores de reputação ficam
**idênticos** aos de antes.

## 7. Manutenção da ordem dos eventos de um mesmo contrato

Já coberta pelo item 3 (mesma partição, offsets crescentes) e pelos logs do item 4 (`particao=N
offset=…` crescente para a mesma `key`). Para evidenciar o paralelismo entre contratos, crie vários
contratos: as chaves se distribuem pelas três partições.

## 8. Tratamento de falhas e DLT

```bash
# falha recuperável simulada: a notificação tenta 3x e desiste
curl -X POST http://localhost:8080/api/contratos -H 'Content-Type: application/json' -H "X-Correlation-Id: $CID-falha" \
  -d '{"clienteId":"11111111-1111-1111-1111-111111111111","freelancerId":"22222222-2222-2222-2222-222222222222","titulo":"FALHA: demonstração DLT","valor":10}'
docker logs --since 2m freela-notificacao 2>&1 | grep -E "evento\.(retry|dlt)"

# mensagem inválida: vai direto para a DLT nos três consumidores
printf 'eventId:ev-x,contratoId:ct-x,correlationId:corr-x\tpoison|{"quebrado"\n' | docker exec -i freela-kafka \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic contratos.eventos \
  --property parse.key=true --property key.separator='|' --property parse.headers=true

docker exec freela-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic contratos.eventos.DLT --from-beginning --timeout-ms 5000 \
  --formatter-property print.key=true --formatter-property print.partition=true --formatter-property print.headers=true
```

Esperado: `evento.retry tentativa=1,2,3` e depois `evento.dlt` na notificação; auditoria e
reputação processam o mesmo evento normalmente; um contrato criado em seguida é processado pela
notificação (ela não fica travada). Na DLT, as mensagens com a chave original e os headers
`kafka_dlt-exception-message`, `kafka_dlt-original-topic`, `kafka_dlt-original-consumer-group`.

## 9. Logs da mesma operação consultados de forma centralizada

Graylog: http://localhost:9000 (admin / admin) → Search, intervalo "Last 1 hour":

```text
correlationId:demo-001
contratoId:<id do contrato>
eventId:<id de um evento>
```

Esperado: linhas de **todos** os serviços da operação numa única consulta, com os campos
`service`, `correlationId`, `contratoId`, `eventId`, `traceId` e `spanId` disponíveis para filtro.
Pela API:

```bash
curl -s -u admin:admin -H 'Accept: application/json' \
  "http://localhost:9000/api/search/universal/relative?query=correlationId:$CID&range=3600&fields=timestamp,service,message"
```

## 10. Trace no Zipkin

Zipkin: http://localhost:9411 → serviceName `api-gateway` → Run query → abrir o trace do `POST /api/contratos`.

Esperado: um único trace com os spans `api-gateway` (http post), `contrato-service`
(`http post /api/contratos`, `outbox publicar`, `contratos.eventos send`) e `contratos.eventos
process` em `notificacao-service`, `reputacao-service` e `auditoria-service`. O `traceId` é o mesmo
que aparece nas linhas de log do item 9 (`traceId=…`).

## Capturas

| Arquivo | Conteúdo |
|---|---|
| `docs/evidencias/01-gateway.png` | resposta 201 e logs do gateway |
| `docs/evidencias/02-contrato-outbox.png` | contrato CONCLUIDO e outbox publicada |
| `docs/evidencias/03-kafka-ordem.png` | mensagens do contrato na mesma partição, em ordem |
| `docs/evidencias/04-consumidores.png` | logs de consumo nos três serviços |
| `docs/evidencias/05-persistencia.png` | notificações, reputação e auditoria via API |
| `docs/evidencias/06-duplicidade.png` | DUPLICADO_IGNORADO e contagens inalteradas |
| `docs/evidencias/07-dlt.png` | retries, DLT e headers de diagnóstico |
| `docs/evidencias/08-graylog.png` | busca por correlationId no Graylog |
| `docs/evidencias/09-zipkin.png` | trace completo no Zipkin |
