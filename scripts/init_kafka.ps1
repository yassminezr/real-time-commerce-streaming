
$ErrorActionPreference = "Stop"

$Topics = @(
    @{ Name = "orders"; Partitions = 3 },
    @{ Name = "payments"; Partitions = 3 },
    @{ Name = "inventory-events"; Partitions = 3 },
    @{ Name = "dead-letter-events"; Partitions = 1 },
    @{ Name = "validated-events"; Partitions = 3 },
    @{ Name = "unique-events"; Partitions = 3 },
    @{ Name = "order-states"; Partitions = 3 }
)

Write-Host "=== INITIALISATION KAFKA ==="

foreach ($topic in $Topics) {
    $name = $topic.Name
    $partitions = $topic.Partitions

    Write-Host "Topic : $name"

    & docker exec rtc-kafka `
        /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server kafka:19092 `
        --create `
        --if-not-exists `
        --topic $name `
        --partitions $partitions `
        --replication-factor 1

    if ($LASTEXITCODE -ne 0) {
        throw "Echec de creation du topic $name"
    }
}

Write-Host ""
Write-Host "[OK] TOPICS KAFKA INITIALISES"
