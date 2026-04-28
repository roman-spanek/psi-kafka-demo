import psycopg2

DATABASE_URL = "postgresql://app:app@localhost:5433/microservices"


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payment_date, total_amount
                FROM report.daily_totals
                ORDER BY payment_date
                """
            )
            rows = cur.fetchall()

    if not rows:
        print("No totals yet")
        return

    for payment_date, total_amount in rows:
        print(f"{payment_date}: {total_amount}")


if __name__ == "__main__":
    main()

