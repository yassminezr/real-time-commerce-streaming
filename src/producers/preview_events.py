
import json

from events import EVENT_TYPES, generate_sample_events


def main():
    events = generate_sample_events()

    print("\n=== EVENEMENTS GENERES ===\n")

    for event in events:
        print(json.dumps(event, indent=2, ensure_ascii=False))
        print("-" * 50)

    # Vérification du nombre d'événements
    assert len(events) == 8

    # Vérification des cinq types métier
    assert {event["event_type"] for event in events} == EVENT_TYPES

    # Chaque événement possède un identifiant unique
    event_ids = [event["event_id"] for event in events]
    assert len(event_ids) == len(set(event_ids))

    # Tous les événements respectent l'enveloppe commune
    required_fields = {
        "event_id",
        "event_type",
        "event_time",
        "schema_version",
        "order_id",
        "customer_id",
        "payload",
    }

    for event in events:
        assert set(event.keys()) == required_fields
        assert event["schema_version"] == 1
        assert event["event_time"].endswith("Z")

        # Vérifie aussi la sérialisation JSON
        json.dumps(event)

    # Les événements d'une commande gardent son customer_id
    customers_by_order = {}

    for event in events:
        order_id = event["order_id"]
        customer_id = event["customer_id"]

        if order_id in customers_by_order:
            assert customers_by_order[order_id] == customer_id
        else:
            customers_by_order[order_id] = customer_id

    assert len(customers_by_order) == 3

    print("\n=== RESULTATS DES TESTS ===")
    print("8 événements générés : OK")
    print("5 types métier présents : OK")
    print("Event IDs uniques : OK")
    print("Structure commune : OK")
    print("Sérialisation JSON : OK")
    print("Cohérence commandes/clients : OK")
    print("\nTOUS LES TESTS SONT PASSES")


if __name__ == "__main__":
    main()
