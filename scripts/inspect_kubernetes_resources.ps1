param(
    [string]$Context = 'docker-desktop',
    [string]$Namespace = 'telemetry-v2'
)
$ErrorActionPreference = 'Stop'
$nodeJson = & kubectl --context $Context --request-timeout=10s get nodes -o json
if ($LASTEXITCODE -ne 0) { throw 'Unable to read nodes.' }
$nodes = $nodeJson | ConvertFrom-Json
$rows = @()
foreach ($node in $nodes.items) {
    $nodeName = $node.metadata.name
    $summaryJson = & kubectl --context $Context --request-timeout=10s get --raw "/api/v1/nodes/$nodeName/proxy/stats/summary"
    if ($LASTEXITCODE -ne 0) { throw "Unable to read kubelet statistics for $nodeName." }
    $summary = $summaryJson | ConvertFrom-Json
    foreach ($pod in $summary.pods) {
        if ($pod.podRef.namespace -eq $Namespace) {
            foreach ($container in $pod.containers) {
                $rows += [pscustomobject]@{
                    Pod = $pod.podRef.name
                    Container = $container.name
                    MemoryMB = [math]::Round($container.memory.workingSetBytes / 1MB, 1)
                    CpuCores = [math]::Round($container.cpu.usageNanoCores / 1e9, 3)
                }
            }
        }
    }
}
$rows | Sort-Object MemoryMB -Descending | Format-Table -AutoSize
$osMemory = Get-CimInstance Win32_OperatingSystem
$projectMemoryMB = [math]::Round(($rows | Measure-Object MemoryMB -Sum).Sum, 1)
Write-Output "Project container working memory: ${projectMemoryMB}MB"
Write-Output "Host available memory: $([math]::Floor($osMemory.FreePhysicalMemory / 1024))MB"
Write-Output 'Container usage excludes Docker/WSL, Kubernetes system pods, Windows applications, and file cache.'
