import json
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "payments.incoming"

SAMPLE_PAYMENTS = [
    {"amount": 10.0, "date": "2026-04-28"},
    {"amount": 15.5, "date": "2026-04-28"},
    {"amount": 9.0, "date": "2026-04-29"},
    {"amount": 0, "date": "2026-04-29"},
]


def main() -> None:
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    for payment in SAMPLE_PAYMENTS:
        producer.send(TOPIC, json.dumps(payment).encode("utf-8"))
        print(f"sent: {payment}")

    producer.flush(timeout=10)
    time.sleep(1)
    print("done")


if __name__ == "__main__":
    main()

