
import json
import sys
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from events import create_event, CURRENCY


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"


def publish(producer, topic, order_id, data, label):
    """Publie un message et verifie sa livraison."""

    results = []

    def delivery_report(error, message):
        if error is not None:
            results.append(False)
            print(f"[ERREUR] {label}: {error}")
        else:
            results.append(True)
            print(
                f"[OK] {label} | "
                f"topic={message.topic()} | "
                f"key={order_id} | "
                f"partition={message.partition()} | "
                f"offset={message.offset()}"
            )

    if isinstance(data, dict):
        value = json.dumps(data).encode("utf-8")
    else:
        # Permet aussi de publier du JSON mal forme
        value = data.encode("utf-8")

    producer.produce(
        topic=topic,
        key=order_id.encode("utf-8"),
        value=value,
        callback=delivery_report,
    )

    remaining = producer.flush(timeout=15)

    if remaining != 0 or results != [True]:
        raise RuntimeError(f"Livraison echouee : {label}")


def main():
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "commerce-anomaly-producer",
        "acks": "all",
    })

    now = datetime.now(timezone.utc)

    try:
        # ==========================================
        # SCENARIO 1 : DUPLICATE EVENT
        # ==========================================
        print("\n=== SCENARIO 1 : DUPLICATE ===")

        duplicate_event = create_event(
            event_type="order_created",
            order_id="ORD-3001",
            customer_id="CUS-301",
            payload={
                "total_amount": 150.0,
                "currency": CURRENCY,
            },
            event_time=now,
        )

        # Meme event_id, meme contenu, meme Kafka Key
        for i in range(2):
            publish(
                producer,
                topic="orders",
                order_id="ORD-3001",
                data=duplicate_event,
                label=f"Duplicate {i + 1}/2",
            )

        # ==========================================
        # SCENARIO 2 : LATE EVENT
        # ==========================================
        print("\n=== SCENARIO 2 : LATE EVENT ===")

        recent_event = create_event(
            event_type="order_created",
            order_id="ORD-3002",
            customer_id="CUS-302",
            payload={
                "total_amount": 200.0,
                "currency": CURRENCY,
            },
            event_time=now,
        )

        late_event = create_event(
            event_type="order_created",
            order_id="ORD-3004",
            customer_id="CUS-304",
            payload={
                "total_amount": 120.0,
                "currency": CURRENCY,
            },
            event_time=now - timedelta(hours=2),
        )

        # Publier l'evenement recent en premier
        publish(
            producer, "orders", "ORD-3002",
            recent_event, "Recent event",
        )

        # Puis un evenement datant de 2 heures
        publish(
            producer, "orders", "ORD-3004",
            late_event, "Late event (-2h)",
        )

        # ==========================================
        # SCENARIO 3 : OUT-OF-ORDER
        # ==========================================
        print("\n=== SCENARIO 3 : OUT-OF-ORDER ===")

        order_event = create_event(
            event_type="order_created",
            order_id="ORD-3003",
            customer_id="CUS-303",
            payload={
                "total_amount": 450.0,
                "currency": CURRENCY,
            },
            event_time=now - timedelta(minutes=3),
        )

        payment_event = create_event(
            event_type="payment_authorized",
            order_id="ORD-3003",
            customer_id="CUS-303",
            payload={
                "payment_id": "PAY-3003",
                "amount": 450.0,
                "currency": CURRENCY,
            },
            event_time=now - timedelta(minutes=2),
        )

        # Publication du paiement AVANT la commande
        publish(
            producer, "payments", "ORD-3003",
            payment_event, "Payment published first",
        )

        publish(
            producer, "orders", "ORD-3003",
            order_event, "Order published second",
        )

        # ==========================================
        # SCENARIO 4 : INVALID EVENTS
        # ==========================================
        print("\n=== SCENARIO 4 : INVALID EVENTS ===")

        # 4A : JSON valide, mais champ obligatoire absent
        invalid_schema = create_event(
            event_type="order_created",
            order_id="ORD-3005",
            customer_id="CUS-305",
            payload={
                "total_amount": 90.0,
                "currency": CURRENCY,
            },
        )

        del invalid_schema["customer_id"]

        publish(
            producer, "orders", "ORD-3005",
            invalid_schema, "Invalid schema",
        )

        # 4B : JSON syntaxiquement incorrect
        malformed_json = (
            '{"event_id":"BROKEN-001",'
            '"event_type":"order_created",'
            '"order_id":"ORD-3006",'
            '"payload":'
        )

        publish(
            producer, "orders", "ORD-3006",
            malformed_json, "Malformed JSON",
        )

        print("\n================================")
        print("8 messages livres a Kafka.")
        print("4 scenarios d'anomalies publies.")
        print("================================")

    except Exception as error:
        print(f"\n[ERREUR] {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
