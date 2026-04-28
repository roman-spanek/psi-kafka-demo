import json
import time

from kafka import KafkaProducer

from config import OUTBOX_BATCH_SIZE, OUTBOX_POLL_INTERVAL_SECONDS, PAYMENT_OUTPUT_TOPIC
from repository import claim_unsent_outbox_rows, mark_outbox_rows_as_sent


class OutboxRelay:
    def __init__(self, conn, producer: KafkaProducer):
        self._conn = conn
        self._producer = producer

    def run_forever(self) -> None:
        while True:
            try:
                rows = claim_unsent_outbox_rows(self._conn, OUTBOX_BATCH_SIZE)
                if not rows:
                    time.sleep(OUTBOX_POLL_INTERVAL_SECONDS)
                    continue

                event_ids = []
                for row in rows:
                    event_id = row[0]          # UUID object from psycopg2
                    payload = row[1]
                    self._producer.send(
                        PAYMENT_OUTPUT_TOPIC,
                        key=str(event_id).encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                    )
                    event_ids.append(event_id)

                self._producer.flush(timeout=10)
                mark_outbox_rows_as_sent(self._conn, event_ids)
                print(f"[payment/outbox] published {len(event_ids)} event(s)")
            except Exception as exc:
                print(f"[payment/outbox] relay error: {exc}")
                time.sleep(OUTBOX_POLL_INTERVAL_SECONDS)

