# Kafka Microservices Demo (Payment + Reports)

This project demonstrates a minimal microservice flow with the **transaction outbox** pattern:

1. `payment` service consumes JSON from Kafka topic `payments.incoming`.
2. If `amount > 0`, it writes payment + outbox event in one PostgreSQL transaction.
3. A relay in the `payment` service publishes unsent outbox events to Kafka topic `payments.validated`.
4. `reports` service consumes validated events and maintains daily totals in PostgreSQL.

## Project layout

- `docker-compose.yml`: local infrastructure + both services.
- `db/init.sql`: schemas/tables for payment and reporting.
- `services/payment`: payment consumer + outbox relay.
- `services/reports`: reports consumer + daily totals aggregation.
- `scripts/send_payments.py`: quick sample producer.
- `scripts/show_report_totals.py`: reads aggregated totals.

## Message contracts

### Incoming payment (`payments.incoming`)

```json
{"amount": 10.5, "date": "2026-04-28"}
```

### Outbox/validated event (`payments.validated`)

```json
{
  "event_id": "uuid",
  "payment_id": "uuid",
  "amount": 10.5,
  "date": "2026-04-28"
}
```

## Run locally

Start everything:

```powershell
docker compose up --build
```

In a second terminal, install helper script dependencies:

```powershell
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

Send sample messages:

```powershell
python scripts/send_payments.py
```

View aggregated totals:

```powershell
python scripts/show_report_totals.py
```

Expected totals after sample messages:

- `2026-04-28`: `25.50`
- `2026-04-29`: `9.00`

(One event has `amount=0` and is intentionally ignored by payment service.)

## Why this demonstrates outbox

- Payment write and outbox write happen in **one DB transaction**.
- Kafka publishing is performed by polling unsent outbox rows.
- If publishing fails, the row remains unsent and is retried.
- Reports service uses `report.processed_events` to deduplicate by `event_id`.

## Stop and clean

```powershell
docker compose down -v
```

