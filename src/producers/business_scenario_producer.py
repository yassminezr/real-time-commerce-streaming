
import json
import sys
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from events import create_event


KAFKA_SERVERS = "127.0.0.1:9092"


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "commerce-phase5-scenarios",
        "acks": "all",
    })

    now = datetime.now(timezone.utc)
    results = []

    def delivery_report(error, message):
        if error is not None:
            results.append(False)
            print(f"[ERREUR] {error}", flush=True)
            return

        results.append(True)
        print(
            f"[OK] {message.topic()} | "
            f"key={message.key().decode('utf-8')} | "
            f"partition={message.partition()} | "
            f"offset={message.offset()}",
            flush=True,
        )

    def event(event_type, order_id, customer_id, payload, seconds):
        return create_event(
            event_type=event_type,
            order_id=order_id,
            customer_id=customer_id,
            payload=payload,
            event_time=now + timedelta(seconds=seconds),
        )

    # Cas 1 : commande reussie
    events = [
        (
            "orders",
            event(
                "order_created", "ORD-P5-001", "CUS-P5-001",
                {"total_amount": 150.0, "currency": "MAD"},
                0,
            ),
        ),
        (
            "payments",
            event(
                "payment_authorized", "ORD-P5-001", "CUS-P5-001",
                {
                    "payment_id": "PAY-P5-001",
                    "amount": 150.0,
                    "currency": "MAD",
                },
                2,
            ),
        ),
        (
            "inventory-events",
            event(
                "inventory_reserved", "ORD-P5-001", "CUS-P5-001",
                {"sku": "SKU-001", "quantity": 1},
                4,
            ),
        ),

        # Cas 2 : paiement refuse
        (
            "orders",
            event(
                "order_created", "ORD-P5-002", "CUS-P5-002",
                {"total_amount": 85.0, "currency": "MAD"},
                10,
            ),
        ),
        (
            "payments",
            event(
                "payment_failed", "ORD-P5-002", "CUS-P5-002",
                {
                    "payment_id": "PAY-P5-002",
                    "amount": 85.0,
                    "currency": "MAD",
                    "reason": "payment_declined",
                },
                12,
            ),
        ),

        # Cas 3 : stock insuffisant
        (
            "orders",
            event(
                "order_created", "ORD-P5-003", "CUS-P5-003",
                {"total_amount": 320.0, "currency": "MAD"},
                20,
            ),
        ),
        (
            "payments",
            event(
                "payment_authorized", "ORD-P5-003", "CUS-P5-003",
                {
                    "payment_id": "PAY-P5-003",
                    "amount": 320.0,
                    "currency": "MAD",
                },
                22,
            ),
        ),
        (
            "inventory-events",
            event(
                "inventory_rejected", "ORD-P5-003", "CUS-P5-003",
                {
                    "sku": "SKU-003",
                    "quantity": 1,
                    "reason": "insufficient_stock",
                },
                24,
            ),
        ),
    ]

    try:
        for topic, message in events:
            producer.produce(
                topic=topic,
                key=message["order_id"].encode("utf-8"),
                value=json.dumps(message).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)

        remaining = producer.flush(timeout=20)

        if remaining or len(results) != 8 or not all(results):
            raise RuntimeError("Certains messages ne sont pas livres")

        print("\n[OK] 8 evenements metier livres a Kafka.")

    except Exception as exc:
        print(f"[ERREUR] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
