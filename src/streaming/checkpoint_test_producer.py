
import json
import sys
from pathlib import Path

from confluent_kafka import Producer

# Reutiliser notre generateur d'evenements
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "producers")
)

from events import create_event


KAFKA_SERVERS = "127.0.0.1:9092"
TOPIC = "orders"

FIXTURE_PATH = (
    Path(__file__).parent / ".checkpoint_test_event.json"
)


def publish(event):
    results = []

    def delivery_report(error, message):
        if error is not None:
            results.append(False)
            print(f"[ERREUR] {error}")
        else:
            results.append(True)
            print(
                f"[OK] Kafka : {message.topic()} "
                f"| partition={message.partition()} "
                f"| offset={message.offset()} "
                f"| key={message.key().decode('utf-8')}"
            )

    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "acks": "all",
    })

    producer.produce(
        TOPIC,
        key=event["order_id"].encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    remaining = producer.flush(15)

    if remaining or results != [True]:
        raise RuntimeError("Livraison Kafka non confirmee")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {
        "first", "duplicate"
    }:
        print("Usage : first ou duplicate")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "first":
        event = create_event(
            event_type="order_created",
            order_id="ORD-4001",
            customer_id="CUS-401",
            payload={
                "total_amount": 175.0,
                "currency": "MAD",
            },
        )

        # Identifiant fixe pour le test de deduplication
        event["event_id"] = "EVT-CHECKPOINT-4001"

        FIXTURE_PATH.write_text(
            json.dumps(event, indent=2),
            encoding="utf-8",
        )

    else:
        if not FIXTURE_PATH.exists():
            raise RuntimeError(
                "Execute d'abord le mode first."
            )

        event = json.loads(
            FIXTURE_PATH.read_text(encoding="utf-8")
        )

    print(
        f"\nMode={mode} "
        f"| order_id={event['order_id']} "
        f"| event_id={event['event_id']}"
    )

    publish(event)


if __name__ == "__main__":
    main()
