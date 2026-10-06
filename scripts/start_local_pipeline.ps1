param(
    [ValidateRange(1,65535)][int]$QueryApiPort = 8080,
    [string]$VehicleBufferDbPath,
    [string]$VehicleBufferDirectory = 'data/runtime/vehicle-buffers',
    [ValidateRange(1,64)][int]$VehicleBufferShards = 8,
    [ValidateRange(1,12)][int]$HistoryConsumerWorkers = 3,
    [ValidateRange(1,12)][int]$RedisConsumerWorkers = 3,
    [string]$LegacyVehicleBufferDbPath = 'data/runtime/expanded_vehicle_buffer.db',
    [string]$SumoConfigPath = 'config/gangnam_expanded_sumo.sumocfg',
    [switch]$StartSumo,
    [ValidateRange(0,86400)][double]$SumoEndSeconds = 0
)
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$pythonPath = Join-Path $projectRoot '.venv\Scripts\python.exe'
$runtimeDirectory = Join-Path $projectRoot 'data\runtime'
$manifestPath = Join-Path $runtimeDirectory 'processes.json'
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Create .venv and install requirements.txt first.' }
if (Test-Path -LiteralPath $manifestPath) {
    foreach ($entry in (Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json)) {
        $running = Get-Process -Id $entry.pid -ErrorAction SilentlyContinue
        if ($running -and $running.StartTime.ToUniversalTime().Ticks.ToString() -eq $entry.startTicks) {
            throw 'A managed pipeline is already running. Stop it with scripts/stop_local_pipeline.ps1 first.'
        }
    }
}
New-Item -ItemType Directory -Path $runtimeDirectory -Force | Out-Null
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONIOENCODING = 'utf-8'
if ($VehicleBufferDbPath) {
    $env:VEHICLE_SQLITE_BUFFER_DB_PATH = if ([IO.Path]::IsPathRooted($VehicleBufferDbPath)) { $VehicleBufferDbPath } else { Join-Path $projectRoot $VehicleBufferDbPath }
}
if ($SumoConfigPath) {
    $env:VEHICLE_SUMO_CONFIG_PATH = if ([IO.Path]::IsPathRooted($SumoConfigPath)) { $SumoConfigPath } else { Join-Path $projectRoot $SumoConfigPath }
}
$definitions = @(@{ name='receiver'; script='server_fleet_telemetry.py'; args=@(); metrics='' })
for ($i=0; $i -lt $HistoryConsumerWorkers; $i++) {
    $definitions += @{ name="history-$i"; script='server_telemetry_consumer.py'; args=@(); metrics="$i" }
}
for ($i=0; $i -lt $RedisConsumerWorkers; $i++) {
    $definitions += @{ name="redis-projection-$i"; script='server_redis_projection_consumer.py'; args=@(); metrics="$i" }
}
$definitions += @{ name='query-api'; script='server_vehicle_query_api.py'; args=@('--query-api-port',"$QueryApiPort"); metrics='' }
if ($VehicleBufferDbPath) {
    $definitions += @{ name='vehicle-client'; script='vehicle_fleet_telemetry_client.py'; args=@(); metrics='0' }
} else {
    $bufferDirectoryPath = if ([IO.Path]::IsPathRooted($VehicleBufferDirectory)) { $VehicleBufferDirectory } else { Join-Path $projectRoot $VehicleBufferDirectory }
    for ($i=0; $i -lt $VehicleBufferShards; $i++) {
        $definitions += @{ name="vehicle-client-$i"; script='vehicle_fleet_telemetry_client.py'; args=@('--vehicle-buffer-shard-index',"$i",'--vehicle-max-in-flight','2'); metrics="$i" }
    }
    if ($LegacyVehicleBufferDbPath) {
        $legacyPath = if ([IO.Path]::IsPathRooted($LegacyVehicleBufferDbPath)) { $LegacyVehicleBufferDbPath } else { Join-Path $projectRoot $LegacyVehicleBufferDbPath }
        if (Test-Path -LiteralPath $legacyPath) {
            $definitions += @{ name='vehicle-client-legacy'; script='vehicle_fleet_telemetry_client.py'; args=@('--vehicle-buffer-db',('"'+$legacyPath+'"'),'--vehicle-max-in-flight','1','--vehicle-sender-max-workers','16'); metrics='legacy'; legacy=$true }
        }
    }
}
if ($StartSumo) {
    $sumoArguments = @('--sumo-binary','sumo')
    if ($SumoEndSeconds -gt 0) { $sumoArguments += @('--sumo-end-seconds',"$SumoEndSeconds") }
    $definitions += @{ name='sumo'; script='vehicle_sumo_collector.py'; args=$sumoArguments; metrics='' }
}
$entries = @()
foreach ($definition in $definitions) {
    $scriptPath = Join-Path $projectRoot ('src\' + $definition.script)
    $arguments = @('-u',('"' + $scriptPath + '"')) + $definition.args
    $env:SHARED_PIPELINE_METRICS_INSTANCE = $definition.metrics
    if (-not $VehicleBufferDbPath -and -not $definition.legacy) {
        $env:VEHICLE_SQLITE_BUFFER_DIRECTORY = $bufferDirectoryPath
        $env:VEHICLE_BUFFER_SHARDS = "$VehicleBufferShards"
    } else {
        $env:VEHICLE_SQLITE_BUFFER_DIRECTORY = ''
    }
    $process = Start-Process -FilePath $pythonPath -ArgumentList $arguments -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $runtimeDirectory ($definition.name+'.log')) -RedirectStandardError (Join-Path $runtimeDirectory ($definition.name+'.error.log'))
    $entries += @{ name=$definition.name; pid=$process.Id; startTicks=$process.StartTime.ToUniversalTime().Ticks.ToString() }
    # Preserve successfully started processes even if a later start fails.
    ConvertTo-Json -InputObject $entries | Set-Content -LiteralPath $manifestPath -Encoding utf8
}
$env:SHARED_PIPELINE_METRICS_INSTANCE = ''
$env:VEHICLE_SQLITE_BUFFER_DIRECTORY = ''
Write-Output "Buffer shards: $VehicleBufferShards; history consumers: $HistoryConsumerWorkers; Redis consumers: $RedisConsumerWorkers"
Write-Output "Pipeline started. Query API: http://127.0.0.1:$QueryApiPort"
Write-Output "Logs and PIDs: $runtimeDirectory"
