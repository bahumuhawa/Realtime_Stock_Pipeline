set -euo pipefail

BOOTSTRAP="kafka:9092"

echo "Waiting for Kafka broker..."
for i in $(seq 1 30); do
  if echo > /dev/tcp/kafka/9092 2>/dev/null; then
    echo "Kafka broker reachable"
    break
  fi
  echo "Kafka not reachable yet, sleeping..."
  sleep 1
done

echo "Creating topics..."
# create main topic
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic market.ticks --partitions 3 --replication-factor 1 || true
kafka-topics --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists --topic market.ticks.dlq --partitions 1 --replication-factor 1 || true

echo "Listing topics:"
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

echo "Kafka topic initialization complete."
