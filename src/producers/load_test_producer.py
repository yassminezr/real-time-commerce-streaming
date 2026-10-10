
import argparse
import json
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer


KAFKA = "127.0.0.1:9092"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--orders",
        type=int,
        default=100,
    )
    args = parser.parse_args()

    if args.orders <= 0 or args.orders % 20 != 0:
        parser.error("--orders doit etre un multiple positif de 20")

    producer = Producer({
        "bootstrap.servers": KAFKA,
        "enable.idempotence": True,
        "acks": "all",
    })

    run_id = uuid.uuid4().hex[:8]
    base_time = datetime.now(timezone.utc)

    counts = {
        "COMPLETED": 0,
        "PAYMENT_FAILED": 0,
        "INVENTORY_REJECTED": 0,
    }

    delivery_errors = []

    def delivery_report(error, message):
        if error is not None:
            delivery_errors.append(str(error))

    def timestamp(value):
        return value.isoformat(
            timespec="milliseconds"
        ).replace("+00:00", "Z")

    def publish(topic, event_type, order_id, customer_id,
                event_time, payload):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_time": timestamp(event_time),
            "schema_version": 1,
            "order_id": order_id,
            "customer_id": customer_id,
            "payload": payload,
        }

        producer.produce(
            topic,
            key=order_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    for i in range(args.orders):
        order_id = f"ORD-LOAD-{run_id}-{i:06d}"
        customer_id = f"CUS-LOAD-{i:06d}"

        order_time = base_time + timedelta(
            milliseconds=i * 20
        )
        payment_time = order_time + timedelta(seconds=2)
        inventory_time = order_time + timedelta(seconds=4)

        amount = float(50 + i % 450)

        # Repartition exacte :
        # 75% succes, 15% paiement refuse,
        # 10% stock refuse.
        scenario = i % 20

        publish(
            "orders",
            "order_created",
            order_id,
            customer_id,
            order_time,
            {
                "total_amount": amount,
                "currency": "MAD",
            },
        )

        if scenario < 15:
            payment_status = "payment_authorized"
            inventory_status = "inventory_reserved"
            final_status = "COMPLETED"

        elif scenario < 18:
            payment_status = "payment_failed"
            inventory_status = None
            final_status = "PAYMENT_FAILED"

        else:
            payment_status = "payment_authorized"
            inventory_status = "inventory_rejected"
            final_status = "INVENTORY_REJECTED"

        payment_payload = {
            "payment_id": f"PAY-{run_id}-{i:06d}",
            "amount": amount,
            "currency": "MAD",
        }

        if payment_status == "payment_failed":
            payment_payload["reason"] = "payment_declined"

        publish(
            "payments",
            payment_status,
            order_id,
            customer_id,
            payment_time,
            payment_payload,
        )

        if inventory_status is not None:
            inventory_payload = {
                "sku": f"SKU-{i % 100:03d}",
                "quantity": 1,
            }

            if inventory_status == "inventory_rejected":
                inventory_payload["reason"] = "insufficient_stock"

            publish(
                "inventory-events",
                inventory_status,
                order_id,
                customer_id,
                inventory_time,
                inventory_payload,
            )

        counts[final_status] += 1

        if (i + 1) % 1000 == 0:
            print(f"[PROGRESS] orders={i + 1}", flush=True)

    remaining = producer.flush(timeout=60)

    if remaining or delivery_errors:
        raise RuntimeError(
            f"Messages non confirmes={remaining}; "
            f"erreurs={delivery_errors[:5]}"
        )

    expected_events = (
        args.orders * 2
        + counts["COMPLETED"]
        + counts["INVENTORY_REJECTED"]
    )

    print("\n=== LOAD TEST DATA GENERATED ===")
    print(f"run_id={run_id}")
    print(f"orders={args.orders}")
    print(f"events_published={expected_events}")

    for status, count in counts.items():
        print(f"{status}={count}")

    print("[OK] GENERATION TERMINEE")


if __name__ == "__main__":
    main()
