import json
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from threading import Thread

from kafka import KafkaConsumer, KafkaProducer

from config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, PAYMENT_INPUT_TOPIC
from outbox_relay import OutboxRelay
from repository import create_connection, save_payment_and_outbox


def parse_payment(raw: bytes) -> tuple[Decimal, date] | None:
    try:
        payload = json.loads(raw.decode("utf-8"))
        amount = Decimal(str(payload["amount"]))
        payment_date = date.fromisoformat(payload["date"])
        if amount <= 0:
            return None
        return amount, payment_date
    except (KeyError, ValueError, InvalidOperation, json.JSONDecodeError) as exc:
        print(f"[payment] invalid input ignored: {exc}")
        return None


def create_kafka_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                PAYMENT_INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id="payment-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            print("[payment] connected to Kafka")
            return consumer
        except Exception as exc:
            print(f"[payment] waiting for Kafka: {exc}")
            time.sleep(2)


def create_kafka_producer() -> KafkaProducer:
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            print("[payment] producer ready")
            return producer
        except Exception as exc:
            print(f"[payment] waiting for Kafka producer: {exc}")
            time.sleep(2)


def main() -> None:
    conn = create_connection(DATABASE_URL)
    relay_conn = create_connection(DATABASE_URL)
    producer = create_kafka_producer()
    relay = OutboxRelay(relay_conn, producer)

    relay_thread = Thread(target=relay.run_forever, daemon=True)
    relay_thread.start()

    consumer = create_kafka_consumer()
    print("[payment] listening for incoming payment JSON")

    while True:
        for message in consumer:
            parsed = parse_payment(message.value)
            if not parsed:
                continue

            amount, payment_date = parsed
            payload = save_payment_and_outbox(conn, amount, payment_date)
            print(f"[payment] accepted payment {payload}")


if __name__ == "__main__":
    main()

