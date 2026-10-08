
import json
import sys

from confluent_kafka import Producer
from events import create_event


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "inventory-events"


def main():
    # Configuration du producteur Kafka
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "commerce-inventory-producer",
        "acks": "all",
    })

    delivery_results = []

    def delivery_report(error, message):
        """Verifie la livraison du message a Kafka."""

        if error is not None:
            delivery_results.append(False)
            print(f"[ERREUR] Livraison Kafka : {error}")
            return

        delivery_results.append(True)

        print("\n[SUCCES] Evenement de stock livre a Kafka")
        print(f"  Topic     : {message.topic()}")
        print(f"  Partition : {message.partition()}")
        print(f"  Offset    : {message.offset()}")
        print(f"  Key       : {message.key().decode('utf-8')}")

    # Scenario 1 : reservation de stock reussie
    inventory_reserved = create_event(
        event_type="inventory_reserved",
        order_id="ORD-2001",
        customer_id="CUS-101",
        payload={
            "sku": "LAPTOP-01",
            "quantity": 1,
        },
    )

    # Scenario 2 : reservation de stock refusee
    inventory_rejected = create_event(
        event_type="inventory_rejected",
        order_id="ORD-1001",
        customer_id="CUS-42",
        payload={
            "sku": "PHONE-02",
            "quantity": 1,
            "reason": "insufficient_stock",
        },
    )

    events = [inventory_reserved, inventory_rejected]

    try:
        for event in events:
            # Conversion de l'evenement en JSON UTF-8
            event_bytes = json.dumps(
                event,
                ensure_ascii=False,
            ).encode("utf-8")

            # L'identifiant de commande est la Kafka Key
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

            producer.poll(0)

        # Attend les confirmations de livraison
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
            print("[ERREUR] Certains evenements n'ont pas ete livres.")
            sys.exit(1)

        print("\n[OK] Les deux evenements de stock ont ete livres.")

    except Exception as error:
        print(f"[ERREUR] Publication impossible : {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
