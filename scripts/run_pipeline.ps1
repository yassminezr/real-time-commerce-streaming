
$ErrorActionPreference = "Stop"

# Se placer a la racine du projet
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$SparkContainer = "rtc-spark-master"
$SparkSubmit = "/opt/spark/bin/spark-submit"

$SparkOptions = @(
    "--master", "local[1]",
    "--driver-memory", "512m",
    "--conf", "spark.jars.ivy=/tmp/spark-ivy",
    "--packages",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
)

function Check-ExitCode {
    param([string]$StepName)

    if ($LASTEXITCODE -ne 0) {
        throw "$StepName a echoue (code $LASTEXITCODE)."
    }
}

function Run-SparkJob {
    param(
        [string]$Name,
        [string]$ScriptPath
    )

    Write-Host ""
    Write-Host "=================================="
    Write-Host " DEMARRAGE : $Name"
    Write-Host "=================================="

    & docker exec $SparkContainer $SparkSubmit `
        @SparkOptions $ScriptPath

    Check-ExitCode $Name

    Write-Host "[OK] $Name"
}

try {
    Write-Host "=== COMMERCE STREAMING PIPELINE ==="

    # Demarrer Kafka
    & docker compose up -d kafka
    Check-ExitCode "Demarrage Kafka"

    # Demarrer Spark
    & docker compose --profile streaming up -d spark-master
    Check-ExitCode "Demarrage Spark"

    # JOB 1
    Run-SparkJob `
        "Validation" `
        "/opt/spark/work-dir/project-src/streaming/integrated/job1_validation.py"

    # JOB 2
    Run-SparkJob `
        "Deduplication" `
        "/opt/spark/work-dir/project-src/streaming/integrated/job2_deduplication.py"

    # JOB 3
    Run-SparkJob `
        "Business Processing" `
        "/opt/spark/work-dir/project-src/streaming/integrated/job3_business_processing.py"

    Write-Host ""
    Write-Host "=================================="
    Write-Host "[OK] PIPELINE EXECUTE AVEC SUCCES"
    Write-Host "=================================="
}
catch {
    Write-Host ""
    Write-Host "[ERROR] $($_.Exception.Message)"
    exit 1
}
