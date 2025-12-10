# producer/producer.py
import os
import time
import json
import logging
import requests
import backoff
from kafka import KafkaProducer
from datetime import datetime, timezone
from signal import signal, SIGINT, SIGTERM

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.ticks")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "market.ticks.dlq")
API_KEY = os.getenv("ALPHA_VANTAGE_KEY", "")
ALPHA_URL = "https://www.alphavantage.co/query"
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "AAPL,MSFT,GOOG").split(",")]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    retries=5,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def now():
    return datetime.now(timezone.utc).isoformat()

@backoff.on_exception(backoff.expo, (requests.exceptions.RequestException,), max_tries=3)
def fetch_price_once(symbol):
    params = {"function": "GLOBAL_QUOTE", "symbol": symbol, "apikey": API_KEY}
    r = requests.get(ALPHA_URL, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_price(symbol):
    try:
        json_data = fetch_price_once(symbol)
        if "Note" in json_data:
            logging.warning(f"Alpha Vantage rate limit note for {symbol}: {json_data.get('Note')}")
            return None
        data = json_data.get("Global Quote")
        if not data:
            logging.warning(f"No data returned for {symbol}")
            return None
        price_raw = data.get("05. price")
        volume_raw = data.get("06. volume")
        if not price_raw:
            logging.warning(f"No price returned for {symbol}")
            return None
        return {
            "event_type": "tick",
            "symbol": symbol.upper(),
            "price": float(price_raw),
            "volume": int(volume_raw) if volume_raw and volume_raw != "" else 0,
            "event_time": now(),
            "received_time": now(),
            "source": "alpha_vantage",
        }
    except requests.HTTPError as e:
        logging.error(f"HTTP error for {symbol}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error for {symbol}: {e}")
        return None

shutdown = False
def _shutdown(*args):
    global shutdown
    logging.info("Shutdown signal received.")
    shutdown = True

signal(SIGINT, _shutdown)
signal(SIGTERM, _shutdown)

def run():
    logging.info("Producer started")
    logging.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP}; symbols={SYMBOLS}")
    sleep_gap = max(1, POLL_INTERVAL / max(1, len(SYMBOLS)))
    while not shutdown:
        for symbol in SYMBOLS:
            if shutdown:
                break
            tick = fetch_price(symbol)
            if tick is None:
                dlq = {"symbol": symbol, "error": "missing_or_invalid_data", "timestamp": now()}
                try:
                    producer.send(DLQ_TOPIC, dlq)
                except Exception as e:
                    logging.error("Failed sending to DLQ: %s", e)
                continue
            try:
                producer.send(KAFKA_TOPIC, tick)
                logging.info("Sent tick: %s", tick)
            except Exception as e:
                logging.error("Kafka send failed for %s: %s", symbol, e)
                try:
                    producer.send(DLQ_TOPIC, {"symbol": symbol, "payload": tick, "error": str(e), "timestamp": now()})
                except Exception as e2:
                    logging.error("Failed to write to DLQ: %s", e2)
            time.sleep(sleep_gap)
        producer.flush()
    logging.info("Flushing and closing producer.")
    producer.flush()
    producer.close()

if __name__ == "__main__":
    run()
