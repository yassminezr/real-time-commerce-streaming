
$ErrorActionPreference = "Stop"

# ============================================================
# REAL-TIME COMMERCE — INTEGRATED STREAMING PIPELINE
# Kafka -> Validation -> Deduplication -> Business -> Cassandra
# ============================================================

# Se placer a la racine du projet
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# Configuration Spark
$SparkContainer = "rtc-spark-master"
$SparkSubmit = "/opt/spark/bin/spark-submit"

$SparkOptions = @(
    "--master", "local[1]",
    "--driver-memory", "512m",
    "--conf", "spark.jars.ivy=/tmp/spark-ivy",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
)

$ScriptDirectory = (
    "/opt/spark/work-dir/project-src/streaming/integrated"
)

# ============================================================
# FONCTIONS
# ============================================================

function Check-ExitCode {
    param([string]$StepName)

    if ($LASTEXITCODE -ne 0) {
        throw "$StepName a echoue (code $LASTEXITCODE)."
    }
}

function Run-SparkJob {
    param(
        [string]$Name,
        [string]$ScriptName
    )

    $ScriptPath = "$ScriptDirectory/$ScriptName"

    Write-Host ""
    Write-Host "========================================"
    Write-Host " DEMARRAGE : $Name"
    Write-Host "========================================"

    & docker exec $SparkContainer $SparkSubmit `
        @SparkOptions $ScriptPath

    Check-ExitCode $Name

    Write-Host "[OK] $Name termine"
}

function Wait-Cassandra {
    Write-Host ""
    Write-Host "Attente de Cassandra..."

    $CassandraReady = $false

    # Maximum 150 secondes
    for ($i = 1; $i -le 30; $i++) {

        $status = & docker exec rtc-cassandra `
            nodetool status 2>$null

        if (
            $LASTEXITCODE -eq 0 -and
            ($status -match '(?m)^\s*UN\s+')
        ) {
            $CassandraReady = $true
            break
        }

        Start-Sleep -Seconds 5
    }

    if (-not $CassandraReady) {
        throw "Cassandra non disponible apres 150 secondes."
    }

    # Verifier egalement que CQL est disponible
    $CqlReady = $false

    for ($i = 1; $i -le 20; $i++) {

        & docker exec rtc-cassandra cqlsh `
            -e "DESCRIBE KEYSPACE commerce;" *> $null

        if ($LASTEXITCODE -eq 0) {
            $CqlReady = $true
            break
        }

        Start-Sleep -Seconds 5
    }

    if (-not $CqlReady) {
        throw "Le keyspace commerce est inaccessible."
    }

    Write-Host "[OK] Cassandra et keyspace commerce operationnels"
}

# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

try {

    Write-Host ""
    Write-Host "========================================"
    Write-Host " REAL-TIME COMMERCE STREAMING PIPELINE"
    Write-Host "========================================"

    # --------------------------------------------------------
    # 1. DEMARRAGE INFRASTRUCTURE
    # --------------------------------------------------------

    Write-Host ""
    Write-Host "[1/7] Demarrage Kafka"

    & docker compose up -d kafka
    Check-ExitCode "Demarrage Kafka"

    Write-Host ""
    Write-Host "[2/7] Demarrage Spark"

    & docker compose --profile streaming up -d spark-master
    Check-ExitCode "Demarrage Spark"

    Write-Host ""
    Write-Host "[3/7] Demarrage Cassandra"

    & docker compose --profile storage up -d cassandra
    Check-ExitCode "Demarrage Cassandra"

    Wait-Cassandra

    # --------------------------------------------------------
    # 2. EXECUTION JOBS SPARK
    # --------------------------------------------------------

    Write-Host ""
    Write-Host "[4/7] JOB 1 : VALIDATION"

    Run-SparkJob `
        "Validation" `
        "job1_validation.py"

    Write-Host ""
    Write-Host "[5/7] JOB 2 : DEDUPLICATION STATEFUL"

    Run-SparkJob `
        "Deduplication" `
        "job2_deduplication.py"

    Write-Host ""
    Write-Host "[6/7] JOB 3 : BUSINESS PROCESSING"

    Run-SparkJob `
        "Business Processing" `
        "job3_business_processing.py"

    Write-Host ""
    Write-Host "[7/7] JOB 4 : CASSANDRA SINK"

    Run-SparkJob `
        "Cassandra Sink" `
        "job4_cassandra_sink.py"

    # --------------------------------------------------------
    # 3. FIN
    # --------------------------------------------------------

    Write-Host ""
    Write-Host "========================================"
    Write-Host "[OK] PIPELINE EXECUTE AVEC SUCCES"
    Write-Host "========================================"

}
catch {

    Write-Host ""
    Write-Host "========================================"
    Write-Host "[ERROR] PIPELINE INTERROMPU"
    Write-Host $_.Exception.Message
    Write-Host "========================================"

    exit 1
}
