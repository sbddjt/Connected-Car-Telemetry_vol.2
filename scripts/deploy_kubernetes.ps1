param(
    [Parameter(Mandatory=$true)][string]$Repository,
    [Parameter(Mandatory=$true)][string]$Version,
    [Parameter(Mandatory=$true)][string]$Context,
    [string]$PythonPath,
    [ValidateSet('laptop','replicated')][string]$Profile = 'laptop',
    [ValidateRange(128,8192)][int]$MinimumAvailableMemoryMB = 512,
    [switch]$StartSimulation
)
$ErrorActionPreference = 'Stop'
if ($Repository -notmatch '^[a-z0-9][a-z0-9_-]*/[a-z0-9][a-z0-9_.-]*$') { throw 'Repository must be Docker Hub account/repository.' }
if ($Version -notmatch '^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,99}$' -or $Version -eq 'latest') { throw 'Use a version tag, such as v0.1.0.' }
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
if (-not $PythonPath) { $PythonPath = Join-Path $projectRoot '.venv/Scripts/python.exe' }
$outputPath = Join-Path $projectRoot ('.build/kubernetes-' + $Profile)
$namespace = 'telemetry-v2'
function Invoke-Cluster {
    & kubectl --context $Context --request-timeout=30s @args
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed: $args" }
}
function Assert-MemoryHeadroom {
    param([int]$UpcomingMemoryMB = 0)
    $osMemory = Get-CimInstance Win32_OperatingSystem
    $availableMB = [math]::Floor($osMemory.FreePhysicalMemory / 1024)
    $requiredMB = $MinimumAvailableMemoryMB + $UpcomingMemoryMB
    Write-Output "Host available memory: ${availableMB}MB; required before next stage: ${requiredMB}MB"
    if ($availableMB -lt $requiredMB) {
        throw "Insufficient headroom for the next stage (${UpcomingMemoryMB}MB budget plus ${MinimumAvailableMemoryMB}MB reserve). Expansion stopped; existing data and workloads are preserved."
    }
}
Invoke-Cluster get nodes
Assert-MemoryHeadroom
# Quorum and replication cannot be silently changed on existing broker PVCs.
$runtimeJson = & kubectl --context $Context --request-timeout=30s -n $namespace get configmap telemetry-runtime --ignore-not-found -o json
if ($LASTEXITCODE -ne 0) { throw 'Unable to inspect existing deployment profile.' }
if ($runtimeJson) {
    $runtime = $runtimeJson | ConvertFrom-Json
    $existingProfile = if ($runtime.data.SERVER_KAFKA_BOOTSTRAP_SERVERS -eq 'kafka-0.kafka:9092') { 'laptop' } else { 'replicated' }
    if ($existingProfile -ne $Profile) { throw 'Profile change requires a separate cluster/namespace and a data migration plan. Existing PVCs are preserved.' }
}
& $PythonPath (Join-Path $PSScriptRoot 'render_kubernetes.py') --image "${Repository}:${Version}" --simulator-image "${Repository}:${Version}-simulator" --output $outputPath --profile $Profile
if ($LASTEXITCODE -ne 0) { throw 'Deployment rendering failed.' }
Invoke-Cluster apply -f (Join-Path $outputPath 'namespace.yaml')
Invoke-Cluster apply -f (Join-Path $outputPath 'config.yaml')
$stageBudgetsMB = if ($Profile -eq 'laptop') {
    @{ kafka = 512; mongodb = 512; redis = 128; topics = 512; servers = 512; simulator = 512 }
} else {
    @{ kafka = 3072; mongodb = 768; redis = 256; topics = 512; servers = 1024; simulator = 1024 }
}
foreach ($workload in @('kafka','mongodb','redis')) {
    Assert-MemoryHeadroom -UpcomingMemoryMB $stageBudgetsMB[$workload]
    Invoke-Cluster apply -f (Join-Path $outputPath "$workload.yaml")
    Invoke-Cluster -n $namespace rollout status "statefulset/$workload" --timeout=10m
}
# Do not automatically delete failed Jobs or persistent data.
Assert-MemoryHeadroom -UpcomingMemoryMB $stageBudgetsMB.topics
Invoke-Cluster apply -f (Join-Path $outputPath 'topics.yaml')
Invoke-Cluster -n $namespace wait --for=condition=complete job/kafka-topics --timeout=10m
Assert-MemoryHeadroom -UpcomingMemoryMB $stageBudgetsMB.servers
Invoke-Cluster apply -f (Join-Path $outputPath 'servers.yaml')
Invoke-Cluster apply -f (Join-Path $outputPath 'simulator.yaml')
foreach ($workload in @('receiver','history-consumer','redis-consumer','query-api')) {
    Invoke-Cluster -n $namespace rollout status "deployment/$workload" --timeout=5m
}
if ($StartSimulation) {
    Assert-MemoryHeadroom -UpcomingMemoryMB $stageBudgetsMB.simulator
    Invoke-Cluster -n $namespace scale statefulset/vehicle-simulator --replicas=1
    Invoke-Cluster -n $namespace rollout status statefulset/vehicle-simulator --timeout=5m
}
Invoke-Cluster -n $namespace get pods,pvc
Write-Output "Open the dashboard with: kubectl --context $Context -n $namespace port-forward service/query-api 8092:8080"
