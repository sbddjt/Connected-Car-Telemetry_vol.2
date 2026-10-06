$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$manifestPath = Join-Path $projectRoot 'data\runtime\processes.json'
if (-not (Test-Path -LiteralPath $manifestPath)) { Write-Output 'No managed pipeline.'; return }
foreach ($entry in (Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json)) {
    $running = Get-Process -Id $entry.pid -ErrorAction SilentlyContinue
    # Start time prevents terminating an unrelated process after PID reuse.
    if (-not $running -or $running.StartTime.ToUniversalTime().Ticks.ToString() -ne $entry.startTicks) { continue }
    $instance = Get-CimInstance Win32_Process -Filter "ProcessId=$($entry.pid)"
    if (-not $instance.CommandLine -or -not $instance.CommandLine.Contains($projectRoot)) { throw "Process ownership mismatch: $($entry.name)" }
    function Stop-OwnedChildren([int]$parentId) {
        foreach ($child in (Get-CimInstance Win32_Process -Filter "ParentProcessId=$parentId")) {
            Stop-OwnedChildren $child.ProcessId
            Stop-Process -Id $child.ProcessId -ErrorAction SilentlyContinue
        }
    }
    Stop-OwnedChildren $entry.pid
    Stop-Process -Id $entry.pid -ErrorAction SilentlyContinue
    Write-Output "Stopped $($entry.name)"
}
# Delete only the verified manifest in this workspace's runtime directory.
Remove-Item -LiteralPath $manifestPath
