import uuid
from datetime import date
from decimal import Decimal

import psycopg2
import psycopg2.extras
from psycopg2.extras import Json

psycopg2.extras.register_uuid()


def create_connection(database_url: str):
    return psycopg2.connect(database_url)


def save_payment_and_outbox(conn, amount: Decimal, payment_date: date) -> dict:
    payment_id = uuid.uuid4()
    event_id = uuid.uuid4()

    payload = {
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "amount": float(amount),
        "date": payment_date.isoformat(),
    }

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO payment.payments (id, amount, payment_date)
                VALUES (%s, %s, %s)
                """,
                (payment_id, amount, payment_date),
            )
            cur.execute(
                """
                INSERT INTO payment.outbox (id, event_type, payload)
                VALUES (%s, %s, %s)
                """,
                (event_id, "payment.accepted", Json(payload)),
            )

    return payload


def claim_unsent_outbox_rows(conn, batch_size: int) -> list[tuple[str, dict]]:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, payload
                FROM payment.outbox
                WHERE sent_at IS NULL
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (batch_size,),
            )
            rows = cur.fetchall()
    return rows


def mark_outbox_rows_as_sent(conn, event_ids: list[str]) -> None:
    if not event_ids:
        return

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE payment.outbox
                SET sent_at = NOW()
                WHERE id = ANY(%s)
                """,
                (event_ids,),
            )

