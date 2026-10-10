
import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer


KAFKA_SERVERS = "127.0.0.1:9092"
TOPIC = "validated-events"


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
    })

    test_id = uuid.uuid4().hex[:8]
    event_time = datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")

    event_a = {
        "event_id": f"DEDUP-{test_id}-A",
        "event_type": "order_created",
        "event_time": event_time,
        "schema_version": 1,
        "order_id": f"ORD-DEDUP-{test_id}-A",
        "customer_id": "CUS-DEDUP-TEST",
        "payload": {
            "total_amount": 100.0,
            "currency": "MAD",
        },
    }

    event_b = {
        **event_a,
        "event_id": f"DEDUP-{test_id}-B",
        "order_id": f"ORD-DEDUP-{test_id}-B",
    }

    messages = [
        event_a,
        event_a.copy(),
        event_b,
    ]

    for event in messages:
        producer.produce(
            TOPIC,
            key=event["order_id"],
            value=json.dumps(event),
        )

    remaining = producer.flush(timeout=15)

    if remaining:
        raise RuntimeError(
            f"{remaining} message(s) non confirmes"
        )

    print("\n=== DEDUP TEST PUBLISHED ===")
    print(f"test_id={test_id}")
    print(f"event_a_id={event_a['event_id']}")
    print(f"event_b_id={event_b['event_id']}")
    print("messages_sent=3")
    print("expected_unique=2")


if __name__ == "__main__":
    main()
