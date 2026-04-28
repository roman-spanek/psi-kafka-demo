import os


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP_SERVERS = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PAYMENT_INPUT_TOPIC = get_env("PAYMENT_INPUT_TOPIC", "payments.incoming")
PAYMENT_OUTPUT_TOPIC = get_env("PAYMENT_OUTPUT_TOPIC", "payments.validated")
DATABASE_URL = get_env("DATABASE_URL")
OUTBOX_POLL_INTERVAL_SECONDS = float(get_env("OUTBOX_POLL_INTERVAL_SECONDS", "1.0"))
OUTBOX_BATCH_SIZE = int(get_env("OUTBOX_BATCH_SIZE", "20"))

