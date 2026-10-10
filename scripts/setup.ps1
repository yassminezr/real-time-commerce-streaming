$ErrorActionPreference = "Stop"

# ==================================================
# REAL-TIME COMMERCE - AUTOMATED SETUP
# ==================================================

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

function Assert-Success {
    param([string]$StepName)

    if ($LASTEXITCODE -ne 0) {
        throw "$StepName a echoue. Code : $LASTEXITCODE"
    }
}

function Wait-Kafka {

    Write-Host "Attente de Kafka..."

    for ($i = 1; $i -le 60; $i++) {

        $ready = $false

        try {
            $output = & docker exec rtc-kafka `
                /opt/kafka/bin/kafka-topics.sh `
                --bootstrap-server kafka:19092 `
                --list 2>&1

            if ($LASTEXITCODE -eq 0) {
                $ready = $true
            }
        }
        catch {
            $ready = $false
        }

        if ($ready) {
            Write-Host "[OK] Kafka operationnel"
            return
        }

        Start-Sleep -Seconds 5
    }

    throw "Kafka non disponible apres 300 secondes"
}

function Wait-Cassandra {

    Write-Host "Attente de Cassandra..."

    for ($i = 1; $i -le 60; $i++) {

        $ready = $false

        try {
            $output = & docker exec rtc-cassandra `
                cqlsh -e "DESCRIBE KEYSPACES;" 2>&1

            if ($LASTEXITCODE -eq 0) {
                $ready = $true
            }
        }
        catch {
            $ready = $false
        }

        if ($ready) {
            Write-Host "[OK] Cassandra operationnel"
            return
        }

        Start-Sleep -Seconds 5
    }

    throw "Cassandra non disponible apres 300 secondes"
}

try {

    Write-Host ""
    Write-Host "========================================"
    Write-Host " REAL-TIME COMMERCE - SETUP"
    Write-Host "========================================"

    # --------------------------------------
    # 1. CONSTRUCTION IMAGE SPARK
    # --------------------------------------

    Write-Host ""
    Write-Host "[1/7] Construction de l'image Spark"

    & docker compose build spark-master
    Assert-Success "Construction Spark"

    # --------------------------------------
    # 2. DEMARRAGE KAFKA
    # --------------------------------------

    Write-Host ""
    Write-Host "[2/7] Demarrage Kafka"

    & docker compose up -d kafka
    Assert-Success "Demarrage Kafka"

    # --------------------------------------
    # 3. DEMARRAGE CASSANDRA
    # --------------------------------------

    Write-Host ""
    Write-Host "[3/7] Demarrage Cassandra"

    & docker compose --profile storage up -d cassandra
    Assert-Success "Demarrage Cassandra"

    # --------------------------------------
    # 4. ATTENTE KAFKA
    # --------------------------------------

    Write-Host ""
    Write-Host "[4/7] Verification Kafka"

    Wait-Kafka

    # --------------------------------------
    # 5. ATTENTE CASSANDRA
    # --------------------------------------

    Write-Host ""
    Write-Host "[5/7] Verification Cassandra"

    Wait-Cassandra

    # --------------------------------------
    # 6. INITIALISATION DES DONNEES
    # --------------------------------------

    Write-Host ""
    Write-Host "[6/7] Initialisation Kafka et Cassandra"

    & powershell.exe -NoProfile -ExecutionPolicy Bypass `
        -File "$PSScriptRoot\init_kafka.ps1"

    Assert-Success "Initialisation Kafka"

    & powershell.exe -NoProfile -ExecutionPolicy Bypass `
        -File "$PSScriptRoot\init_cassandra.ps1"

    Assert-Success "Initialisation Cassandra"

    # --------------------------------------
    # 7. DEMARRAGE SPARK
    # --------------------------------------

    Write-Host ""
    Write-Host "[7/7] Demarrage Spark Master"

    & docker compose --profile streaming up -d `
        --no-deps spark-master

    Assert-Success "Demarrage Spark"

    Write-Host ""
    Write-Host "========================================"
    Write-Host "[OK] ENVIRONNEMENT INITIALISE"
    Write-Host "========================================"

}
catch {

    Write-Host ""
    Write-Host "[ERROR] $($_.Exception.Message)"
    exit 1
}