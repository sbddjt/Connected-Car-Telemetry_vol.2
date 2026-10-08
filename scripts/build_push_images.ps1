param(
    [Parameter(Mandatory=$true)][string]$Repository,
    [Parameter(Mandatory=$true)][string]$Version,
    [switch]$Push
)
$ErrorActionPreference = 'Stop'
if ($Repository -notmatch '^[a-z0-9][a-z0-9_-]*/[a-z0-9][a-z0-9_.-]*$') { throw 'Repository must be Docker Hub account/repository.' }
if ($Version -notmatch '^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,99}$' -or $Version -eq 'latest') { throw 'Use a version tag, such as v0.1.0.' }
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$serverImage = "${Repository}:${Version}"
$simulatorImage = "${Repository}:${Version}-simulator"
& docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop (Linux containers) first.' }
& docker build --target server --tag $serverImage $projectRoot
if ($LASTEXITCODE -ne 0) { throw 'Server image build failed.' }
& docker build --target simulator --tag $simulatorImage $projectRoot
if ($LASTEXITCODE -ne 0) { throw 'Simulator image build failed.' }
foreach ($role in @('receiver','history-consumer','redis-consumer','query-api','vehicle-sender')) {
    & docker run --rm --memory=128m $serverImage $role --help
    if ($LASTEXITCODE -ne 0) { throw "Container role check failed: $role" }
}
& docker run --rm --memory=256m $simulatorImage vehicle-collector --help
if ($LASTEXITCODE -ne 0) { throw 'Collector role check failed.' }
& docker run --rm --entrypoint sumo $simulatorImage --version
if ($LASTEXITCODE -ne 0) { throw 'SUMO executable check failed.' }
& docker run --rm --memory=256m --cpus=0.5 --tmpfs /app/data:uid=10001,gid=10001 -e VEHICLE_BUFFER_OVERFLOW_POLICY=block $simulatorImage vehicle-collector --sumo-max-vehicles 5 --sumo-end-seconds 3
if ($LASTEXITCODE -ne 0) { throw 'SUMO scenario execution check failed.' }
if ($Push) {
    & docker push $serverImage
    if ($LASTEXITCODE -ne 0) { throw 'Server image upload failed. Check registry connectivity and local login.' }
    & docker push $simulatorImage
    if ($LASTEXITCODE -ne 0) { throw 'Simulator image upload failed.' }
}
Write-Output "Server: $serverImage"
Write-Output "Simulator: $simulatorImage"
