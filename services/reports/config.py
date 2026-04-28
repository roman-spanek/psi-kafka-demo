import os


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


KAFKA_BOOTSTRAP_SERVERS = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REPORTS_INPUT_TOPIC = get_env("REPORTS_INPUT_TOPIC", "payments.validated")
REPORTS_CONSUMER_GROUP = get_env("REPORTS_CONSUMER_GROUP", "reports-consumer")
DATABASE_URL = get_env("DATABASE_URL")

