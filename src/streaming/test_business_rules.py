
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit


BASE_TIME = datetime(2026, 10, 9, 1, 52, 11)


def make_test_data(spark, extra_payment=None):
    """Cree trois scenarios metier connus."""

    orders = spark.createDataFrame(
        [
            ("ORD-P5-001", BASE_TIME),
            ("ORD-P5-002", BASE_TIME + timedelta(seconds=10)),
            ("ORD-P5-003", BASE_TIME + timedelta(seconds=20)),
        ],
        "order_id string, order_time timestamp",
    )

    payment_rows = [
        (
            "ORD-P5-001", "PAY-P5-001",
            "payment_authorized",
            BASE_TIME + timedelta(seconds=2),
        ),
        (
            "ORD-P5-002", "PAY-P5-002",
            "payment_failed",
            BASE_TIME + timedelta(seconds=12),
        ),
        (
            "ORD-P5-003", "PAY-P5-003",
            "payment_authorized",
            BASE_TIME + timedelta(seconds=22),
        ),
    ]

    if extra_payment is not None:
        payment_rows.append(extra_payment)

    payments = spark.createDataFrame(
        payment_rows,
        """order_id string, payment_id string,
           payment_status string, payment_time timestamp""",
    )

    inventory = spark.createDataFrame(
        [
            (
                "ORD-P5-001", "inventory_reserved",
                BASE_TIME + timedelta(seconds=4),
            ),
            (
                "ORD-P5-003", "inventory_rejected",
                BASE_TIME + timedelta(seconds=24),
            ),
        ],
        """order_id string, inventory_status string,
           inventory_time timestamp""",
    )

    return orders, payments, inventory


def correlate(orders, payments, inventory):
    """Teste les regles de correlation utilisees en Phase 5."""

    # Jointure 1 : Orders + Payments
    op = (
        orders.alias("o")
        .join(
            payments.alias("p"),
            expr("""
                o.order_id = p.order_id
                AND p.payment_time >= o.order_time
                AND p.payment_time <=
                    o.order_time + INTERVAL 15 MINUTES
            """),
            "inner",
        )
        .select(
            col("o.order_id").alias("order_id"),
            col("p.payment_id").alias("payment_id"),
            col("p.payment_status").alias("payment_status"),
            col("p.payment_time").alias("payment_time"),
        )
    )

    # Paiements refuses : conserver sans Inventory
    failed = (
        op.filter(col("payment_status") == "payment_failed")
        .select(
            "order_id",
            "payment_id",
            "payment_status",
            lit(None).cast("string").alias("inventory_status"),
        )
    )

    # Paiements autorises : chercher Inventory
    authorized = (
        op.filter(col("payment_status") == "payment_authorized")
        .alias("op")
    )

    with_inventory = (
        authorized
        .join(
            inventory.alias("i"),
            expr("""
                op.order_id = i.order_id
                AND i.inventory_time >= op.payment_time
                AND i.inventory_time <=
                    op.payment_time + INTERVAL 15 MINUTES
            """),
            "inner",
        )
        .select(
            col("op.order_id").alias("order_id"),
            col("op.payment_id").alias("payment_id"),
            col("op.payment_status").alias("payment_status"),
            col("i.inventory_status").alias("inventory_status"),
        )
    )

    return failed.unionByName(with_inventory)


def test_all(spark):
    print("\n=== TEST 1 : SCENARIOS NORMAUX ===", flush=True)

    orders, payments, inventory = make_test_data(spark)
    result = correlate(orders, payments, inventory)
    rows = result.collect()

    assert len(rows) == 3, f"3 lignes attendues, {len(rows)} obtenues"

    by_order = {r["order_id"]: r for r in rows}

    assert by_order["ORD-P5-001"]["inventory_status"] == (
        "inventory_reserved"
    )
    assert by_order["ORD-P5-002"]["payment_status"] == (
        "payment_failed"
    )
    assert by_order["ORD-P5-003"]["inventory_status"] == (
        "inventory_rejected"
    )

    print("[PASS] 3 scenarios metier corrects", flush=True)

    print("\n=== TEST 2 : PAIEMENT HORS FENETRE ===", flush=True)

    late_payment = (
        "ORD-P5-001",
        "PAY-LATE",
        "payment_authorized",
        BASE_TIME + timedelta(minutes=16),
    )

    o, p, i = make_test_data(spark, late_payment)
    rows = correlate(o, p, i).collect()

    payment_ids = [r["payment_id"] for r in rows]

    assert "PAY-LATE" not in payment_ids
    assert len(rows) == 3

    print("[PASS] Paiement a +16 minutes exclu", flush=True)

    print("\n=== TEST 3 : INVENTORY MANQUANT ===", flush=True)

    failed_order = [
        r for r in rows
        if r["order_id"] == "ORD-P5-002"
    ]

    assert len(failed_order) == 1
    assert failed_order[0]["inventory_status"] is None

    print(
        "[PASS] ORD-P5-002 conserve avec Inventory NULL",
        flush=True,
    )

    print("\n=== TEST 4 : REPETITION METIER ===", flush=True)

    second_payment = (
        "ORD-P5-001",
        "PAY-P5-001-B",
        "payment_authorized",
        BASE_TIME + timedelta(seconds=3),
    )

    o, p, i = make_test_data(spark, second_payment)
    rows = correlate(o, p, i).collect()

    order_001 = [
        r for r in rows
        if r["order_id"] == "ORD-P5-001"
    ]

    assert len(order_001) == 2
    assert len(rows) == 4

    print(
        "[PASS] Deux paiements distincts produisent "
        "deux correspondances possibles",
        flush=True,
    )

    print(
        "\n[OK] Tous les tests metier ont reussi.",
        flush=True,
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Commerce-Phase5-Business-Tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        test_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
