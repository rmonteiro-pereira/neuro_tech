# PowerShell script to stop/pause all running Airflow DAGs

Write-Host "=== Stopping All Airflow DAGs ===" -ForegroundColor Cyan
Write-Host ""

# Get all DAGs and pause them
Write-Host "Pausing all DAGs..." -ForegroundColor Yellow
$dags = docker exec airflow-scheduler airflow dags list 2>&1 | Select-String -Pattern "^\w+\s+" | ForEach-Object {
    ($_ -split '\s+')[0]
} | Where-Object { $_ -ne "dag_id" -and $_ -ne "" }

foreach ($dag_id in $dags) {
    Write-Host "Pausing DAG: $dag_id" -ForegroundColor Gray
    docker exec airflow-scheduler airflow dags pause $dag_id 2>$null
}

Write-Host ""
Write-Host "Stopping all running task instances..." -ForegroundColor Yellow

# Clear all running tasks for the IPTU DAG (you can modify this for other DAGs)
docker exec airflow-scheduler airflow tasks clear iptu_medallion_pipeline --state running -y 2>$null

Write-Host ""
Write-Host "=== All DAGs Paused ===" -ForegroundColor Green
Write-Host ""
Write-Host "To unpause a specific DAG, run:" -ForegroundColor Yellow
Write-Host "  docker exec airflow-scheduler airflow dags unpause <dag_id>"
Write-Host ""
Write-Host "To unpause all DAGs, run:" -ForegroundColor Yellow
Write-Host "  docker exec airflow-scheduler airflow dags list | Select-String -Pattern '^\w+\s+' | ForEach-Object { `$dag = (`$_ -split '\s+')[0]; if (`$dag -ne 'dag_id') { docker exec airflow-scheduler airflow dags unpause `$dag } }"

