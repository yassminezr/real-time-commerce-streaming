
import json
import sys

from confluent_kafka import Producer
from events import create_event, CURRENCY


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "payments"


def main():
    # Configuration du Kafka Producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "commerce-payment-producer",
        "acks": "all",
    })

    # Suivi des confirmations de livraison
    delivery_results = []

    def delivery_report(error, message):
        if error is not None:
            delivery_results.append(False)
            print(f"[ERREUR] Livraison Kafka : {error}")
            return

        delivery_results.append(True)

        print("[SUCCES] Paiement livre a Kafka")
        print(f"  Topic     : {message.topic()}")
        print(f"  Partition : {message.partition()}")
        print(f"  Offset    : {message.offset()}")
        print(f"  Key       : {message.key().decode('utf-8')}")

    # Scenario 1 : paiement autorise
    payment_authorized = create_event(
        event_type="payment_authorized",
        order_id="ORD-2001",
        customer_id="CUS-101",
        payload={
            "payment_id": "PAY-2001",
            "amount": 349.90,
            "currency": CURRENCY,
        },
    )

    # Scenario 2 : paiement refuse
    payment_failed = create_event(
        event_type="payment_failed",
        order_id="ORD-1001",
        customer_id="CUS-42",
        payload={
            "payment_id": "PAY-1001",
            "amount": 249.90,
            "currency": CURRENCY,
            "reason": "payment_declined",
        },
    )

    events = [payment_authorized, payment_failed]

    try:
        for event in events:
            # Serialisation JSON en bytes
            event_bytes = json.dumps(
                event,
                ensure_ascii=False,
            ).encode("utf-8")

            # Meme Kafka Key que la commande
            kafka_key = event["order_id"].encode("utf-8")

            print(
                f"\nPublication : {event['event_type']}"
                f" | order_id={event['order_id']}"
            )

            producer.produce(
                topic=KAFKA_TOPIC,
                key=kafka_key,
                value=event_bytes,
                callback=delivery_report,
            )

            # Declenche le traitement des callbacks disponibles
            producer.poll(0)

        # Attend les confirmations des deux messages
        remaining = producer.flush(timeout=15)

        if remaining > 0:
            print(
                f"[ERREUR] {remaining} message(s) encore en attente."
            )
            sys.exit(1)

        if (
            len(delivery_results) != len(events)
            or not all(delivery_results)
        ):
            print("[ERREUR] Certains paiements n'ont pas ete livres.")
            sys.exit(1)

        print("\n[OK] Les deux paiements ont ete livres a Kafka.")

    except Exception as error:
        print(f"[ERREUR] Publication impossible : {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
