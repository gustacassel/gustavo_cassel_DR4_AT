#!/bin/sh
# Cria o input GELF UDP no Graylog (uma vez) para receber os logs dos microsserviços.
API=http://graylog:9000/api
AUTH="admin:admin"

echo "aguardando o Graylog..."
until curl -sf "$API/system/lbstatus" | grep -q ALIVE; do sleep 5; done

if curl -s -u "$AUTH" "$API/system/inputs" | grep -q GELFUDPInput; then
  echo "input GELF UDP ja existe"
  exit 0
fi

curl -s -u "$AUTH" -X POST "$API/system/inputs" \
  -H "Content-Type: application/json" -H "X-Requested-By: freela-init" \
  -d '{
    "title": "GELF UDP (microsservicos)",
    "type": "org.graylog2.inputs.gelf.udp.GELFUDPInput",
    "global": true,
    "configuration": {
      "bind_address": "0.0.0.0",
      "port": 12201,
      "recv_buffer_size": 262144,
      "number_worker_threads": 2,
      "decompress_size_limit": 8388608,
      "charset_name": "UTF-8"
    }
  }'
echo
echo "input GELF UDP criado"
