from datetime import date
from decimal import Decimal
from uuid import UUID

import psycopg2
import psycopg2.extras

psycopg2.extras.register_uuid()


def create_connection(database_url: str):
    return psycopg2.connect(database_url)


def apply_payment_event(conn, event_id: UUID, amount: Decimal, payment_date: date) -> bool:
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO report.processed_events (event_id)
                VALUES (%s)
                ON CONFLICT DO NOTHING
                """,
                (event_id,),
            )

            if cur.rowcount == 0:
                return False

            cur.execute(
                """
                INSERT INTO report.daily_totals (payment_date, total_amount)
                VALUES (%s, %s)
                ON CONFLICT (payment_date)
                DO UPDATE SET
                    total_amount = report.daily_totals.total_amount + EXCLUDED.total_amount,
                    updated_at = NOW()
                """,
                (payment_date, amount),
            )
    return True

