# 🎓 Demo Walkthrough – Kafka Microservices + Outbox Pattern

## Prerequisites
- Docker Desktop running
- Python 3.10+ installed

---

## Step 1 – Start the infrastructure + services

```powershell
cd C:\dev\teaching\python\psi-kafka-demo
docker compose up --build
```

Wait until you see both:
```
payment-service  | [payment] listening for incoming payment JSON
reports-service  | [reports] listening for payment events
```

> ⏱ First boot takes ~60 s while Kafka and Postgres become healthy.

---

## Step 2 – Open split terminal windows

| Terminal | Purpose |
|---|---|
| **A** | `docker compose up` output – watch all service logs |
| **B** | Send payment messages |
| **C** | Query the reports DB |

---

## Step 3 – Install helper script dependencies (once)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## Step 4 – Send sample payments (Terminal B)

```powershell
python scripts/send_payments.py
```

This produces **4 messages** to `payments.incoming`:

| amount | date       | accepted? |
|--------|------------|-----------|
| 10.00  | 2026-04-28 | ✅ yes    |
| 15.50  | 2026-04-28 | ✅ yes    |
| 9.00   | 2026-04-29 | ✅ yes    |
| 0.00   | 2026-04-29 | ❌ no (amount ≤ 0) |

---

## Step 5 – Watch logs in Terminal A

You should see:

```
payment-service  | [payment] accepted payment {'amount': 10.0, 'date': '2026-04-28', ...}
payment-service  | [payment/outbox] published 1 event(s)
reports-service  | [reports] applied event event_id=... amount=10.0 date=2026-04-28
...
```

### 🔍 Teaching points to highlight here:
1. **Payment writes to DB and outbox in one transaction** – if Kafka were down, data is safe in the outbox.
2. **Relay reads outbox and publishes to Kafka** – decouples DB write from Kafka publish.
3. **Reports consumes Kafka and deduplicates by event_id** – idempotent consumer.

---

## Step 6 – View daily totals (Terminal C)

```powershell
python scripts/show_report_totals.py
```

Expected output:
```
2026-04-28: 25.50
2026-04-29: 9.00
```

---

## Step 7 – Simulate outbox failure resilience

### Kill Kafka temporarily, then send payments:

```powershell
# Stop Kafka only
docker compose stop kafka

# Send more payments – payment service will STILL accept them into DB+outbox
python scripts/send_payments.py

# Restart Kafka
docker compose start kafka

# Watch the outbox relay publish the buffered events
# Check totals again
python scripts/show_report_totals.py
```

🎯 **This is the core outbox pattern proof**: payments were stored safely in Postgres even when Kafka was down, and published once Kafka came back.

---

## Step 8 – Simulate duplicate event (idempotency demo)

Run `send_payments.py` a second time. Because `reports` deduplicates by `event_id`, totals will NOT change.

```powershell
python scripts/send_payments.py   # second run
python scripts/show_report_totals.py  # same totals as before
```

Watch the logs:
```
reports-service | [reports] duplicate event ignored: <uuid>
```

---

## Step 9 – Inspect Postgres directly

```powershell
docker exec -it demo-postgres psql -U app -d microservices
```

Useful queries:
```sql
-- Accepted payments
SELECT * FROM payment.payments;

-- Outbox state (sent_at NULL = not yet published)
SELECT id, event_type, sent_at FROM payment.outbox;

-- Daily totals in reports service
SELECT * FROM report.daily_totals;

-- Processed (deduplicated) events
SELECT * FROM report.processed_events;
```

---

## Step 10 – Clean up

```powershell
docker compose down -v
```

> `-v` removes the Postgres volume so the next demo starts fresh.

---

## Architecture summary

```
[send_payments.py]
      │
      ▼  JSON {amount, date}
┌─────────────────────┐
│  PAYMENT SERVICE    │
│  - validates amount │
│  - writes to DB     │◄──── payments table (Postgres)
│  - writes outbox    │◄──── outbox table  (same TX)
│  - relay publishes  │────► payments.validated (Kafka)
└─────────────────────┘
              │
              ▼ Kafka message
┌─────────────────────┐
│  REPORTS SERVICE    │
│  - deduplicates     │◄──── processed_events table
│  - upserts totals   │◄──── daily_totals table
└─────────────────────┘
```
