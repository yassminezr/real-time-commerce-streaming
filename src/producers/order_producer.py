
import json
import sys

from confluent_kafka import Producer
from events import create_event, CURRENCY


KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "orders"


def delivery_report(error, message):
    """Affiche le résultat de livraison d'un message Kafka."""

    if error is not None:
        print(f"[ERREUR] Livraison Kafka : {error}")
        return

    print("[SUCCES] Evenement livre a Kafka")
    print(f"  Topic     : {message.topic()}")
    print(f"  Partition : {message.partition()}")
    print(f"  Offset    : {message.offset()}")
    print(f"  Key       : {message.key().decode('utf-8')}")


def main():
    # Configuration du producteur Kafka
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "commerce-order-producer",
        "acks": "all",
    })

    # Generation d'un evenement metier
    event = create_event(
        event_type="order_created",
        order_id="ORD-2001",
        customer_id="CUS-101",
        payload={
            "total_amount": 349.90,
            "currency": CURRENCY,
        },
    )

    # Serialisation JSON en bytes UTF-8
    event_json = json.dumps(event, ensure_ascii=False)
    event_bytes = event_json.encode("utf-8")

    # La Kafka Key est l'identifiant de commande
    kafka_key = event["order_id"].encode("utf-8")

    print("Publication de l'evenement :")
    print(json.dumps(event, indent=2, ensure_ascii=False))

    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=kafka_key,
            value=event_bytes,
            callback=delivery_report,
        )

        # Attend le traitement des messages en attente
        remaining = producer.flush(timeout=15)

        if remaining > 0:
            print(
                f"[ERREUR] {remaining} message(s) non livres "
                "avant le timeout."
            )
            sys.exit(1)

    except Exception as error:
        print(f"[ERREUR] Publication impossible : {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
