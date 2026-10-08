
import uuid
from datetime import datetime, timezone, timedelta


SCHEMA_VERSION = 1
CURRENCY = "MAD"

EVENT_TYPES = {
    "order_created",
    "payment_authorized",
    "payment_failed",
    "inventory_reserved",
    "inventory_rejected",
}


def create_event(
    event_type,
    order_id,
    customer_id,
    payload,
    event_time=None,
):
    """Construit un événement respectant notre contrat V1."""

    if event_type not in EVENT_TYPES:
        raise ValueError(f"Type d'événement inconnu : {event_type}")

    if not order_id or not customer_id:
        raise ValueError("order_id et customer_id sont obligatoires")

    timestamp = event_time or datetime.now(timezone.utc)

    if timestamp.tzinfo is None:
        raise ValueError("event_time doit contenir un fuseau horaire")

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": timestamp.astimezone(
            timezone.utc
        ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "schema_version": SCHEMA_VERSION,
        "order_id": order_id,
        "customer_id": customer_id,
        "payload": payload,
    }


def generate_sample_events():
    """Génère trois parcours métier cohérents."""

    base_time = datetime.now(timezone.utc)
    events = []

    def at(seconds):
        return base_time + timedelta(seconds=seconds)

    # Scénario 1 : commande réussie
    order_id = "ORD-1001"
    customer_id = "CUS-42"

    events.append(create_event(
        "order_created", order_id, customer_id,
        {"total_amount": 249.90, "currency": CURRENCY},
        at(0),
    ))

    events.append(create_event(
        "payment_authorized", order_id, customer_id,
        {
            "payment_id": "PAY-1001",
            "amount": 249.90,
            "currency": CURRENCY,
        },
        at(2),
    ))

    events.append(create_event(
        "inventory_reserved", order_id, customer_id,
        {"sku": "LAPTOP-01", "quantity": 1},
        at(4),
    ))

    # Scénario 2 : paiement refusé
    order_id = "ORD-1002"
    customer_id = "CUS-17"

    events.append(create_event(
        "order_created", order_id, customer_id,
        {"total_amount": 89.00, "currency": CURRENCY},
        at(10),
    ))

    events.append(create_event(
        "payment_failed", order_id, customer_id,
        {
            "payment_id": "PAY-1002",
            "amount": 89.00,
            "currency": CURRENCY,
            "reason": "payment_declined",
        },
        at(12),
    ))

    # Scénario 3 : stock insuffisant
    order_id = "ORD-1003"
    customer_id = "CUS-26"

    events.append(create_event(
        "order_created", order_id, customer_id,
        {"total_amount": 399.00, "currency": CURRENCY},
        at(20),
    ))

    events.append(create_event(
        "payment_authorized", order_id, customer_id,
        {
            "payment_id": "PAY-1003",
            "amount": 399.00,
            "currency": CURRENCY,
        },
        at(22),
    ))

    events.append(create_event(
        "inventory_rejected", order_id, customer_id,
        {
            "sku": "PHONE-02",
            "quantity": 1,
            "reason": "insufficient_stock",
        },
        at(24),
    ))

    return events
