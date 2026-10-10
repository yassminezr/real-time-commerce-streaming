$ErrorActionPreference = "Stop"

# ==================================================
# INITIALISATION CASSANDRA
# ==================================================

Write-Host "=== INITIALISATION CASSANDRA ==="

# Creation du keyspace
$keyspaceCql = @"
CREATE KEYSPACE IF NOT EXISTS commerce
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
"@

& docker exec rtc-cassandra cqlsh -e $keyspaceCql

if ($LASTEXITCODE -ne 0) {
    throw "Echec de creation du keyspace commerce"
}

Write-Host "[OK] Keyspace commerce"

# Creation de la table
$tableCql = @"
CREATE TABLE IF NOT EXISTS commerce.order_state_history (
    order_id text,
    state_id text,
    customer_id text,
    inventory_status text,
    order_amount double,
    order_status text,
    order_time timestamp,
    payment_status text,
    PRIMARY KEY ((order_id), state_id)
);
"@

& docker exec rtc-cassandra cqlsh -e $tableCql

if ($LASTEXITCODE -ne 0) {
    throw "Echec de creation de order_state_history"
}

Write-Host "[OK] Table order_state_history"

Write-Host ""
Write-Host "[OK] CASSANDRA INITIALISE"