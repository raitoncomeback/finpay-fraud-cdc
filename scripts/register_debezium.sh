#!/usr/bin/env bash
# Register Debezium connector for FinPay CDC

set -e

CONNECT_URL="http://localhost:8083/connectors"
CONNECTOR_NAME="finpay-cdc"

echo "Waiting for Debezium Connect to be ready..."
until curl -s -f "$CONNECT_URL" > /dev/null; do
    sleep 2
done

echo "Registering connector: $CONNECTOR_NAME"

curl -X PUT -H "Content-Type: application/json" "$CONNECT_URL/$CONNECTOR_NAME/config" -d '{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "finpay-postgres",
  "database.port": "5432",
  "database.user": "finpay",
  "database.password": "finpay123",
  "database.dbname": "finpay",
  "database.server.name": "finpay",
  "table.include.list": "public.users,public.accounts,public.merchants,public.merchant_locations,public.devices,public.transactions,public.fraud_signals",
  "publication.name": "finpay_cdc",
  "slot.name": "finpay_cdc_slot",
  "plugin.name": "pgoutput",
  "slot.drop.on.stop": "false",
  "transforms": "unwrap,addSource",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.addSource.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addSource.static.field": "cdc_source",
  "transforms.addSource.static.value": "finpay-postgres",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "key.converter.schemas.enable": "false",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false",
  "topic.prefix": "finpay",
  "snapshot.mode": "initial",
  "snapshot.locking.mode": "none",
  "decimal.handling.mode": "string",
  "time.precision.mode": "connect",
  "max.batch.size": "2048",
  "max.queue.size": "8192",
  "poll.interval.ms": "100",
  "include.schema.changes": "false"
}'

echo ""
echo "Connector registered. Checking status..."
sleep 3
curl -s "$CONNECT_URL/$CONNECTOR_NAME/status" | jq .

echo ""
echo "Topics created:"
curl -s "http://localhost:9092/kafka-topics?list" 2>/dev/null | grep finpay || echo "Use Kafka UI at http://localhost:8090 to view topics"