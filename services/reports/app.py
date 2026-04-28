import json
import time
from datetime import date
from decimal import Decimal, InvalidOperation
from uuid import UUID

from kafka import KafkaConsumer

from config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS, REPORTS_CONSUMER_GROUP, REPORTS_INPUT_TOPIC
from repository import apply_payment_event, create_connection


def parse_event(raw: bytes) -> tuple[UUID, Decimal, date] | None:
    try:
        payload = json.loads(raw.decode("utf-8"))
        event_id = UUID(payload["event_id"])
        amount = Decimal(str(payload["amount"]))
        payment_date = date.fromisoformat(payload["date"])
        return event_id, amount, payment_date
    except (KeyError, ValueError, InvalidOperation, json.JSONDecodeError) as exc:
        print(f"[reports] invalid event ignored: {exc}")
        return None


def create_kafka_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                REPORTS_INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=REPORTS_CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
            )
            print("[reports] connected to Kafka")
            return consumer
        except Exception as exc:
            print(f"[reports] waiting for Kafka: {exc}")
            time.sleep(2)


def main() -> None:
    conn = create_connection(DATABASE_URL)
    consumer = create_kafka_consumer()

    print("[reports] listening for payment events")

    while True:
        for message in consumer:
            parsed = parse_event(message.value)
            if not parsed:
                continue

            event_id, amount, payment_date = parsed
            is_new = apply_payment_event(conn, event_id, amount, payment_date)
            if is_new:
                print(
                    "[reports] applied event "
                    f"event_id={event_id} amount={amount} date={payment_date}"
                )
            else:
                print(f"[reports] duplicate event ignored: {event_id}")


if __name__ == "__main__":
    main()

