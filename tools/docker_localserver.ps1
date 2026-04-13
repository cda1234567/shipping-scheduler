param(
    [ValidateSet("start", "restart")]
    [string]$Action = "start"
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$containerName = "dispatch-scheduler-localserver"
Push-Location $repoRoot

try {
    $containerExists = $false
    $isRunning = $false
    $stateOutput = docker inspect -f "{{.State.Running}}" $containerName 2>$null
    if ($LASTEXITCODE -eq 0) {
        $containerExists = $true
        $isRunning = (("$stateOutput" | Select-Object -First 1).Trim().ToLower() -eq "true")
    }

    if ($Action -eq "restart") {
        if ($containerExists) {
            docker restart $containerName
        }
        else {
            docker compose --env-file ".env.localserver" -f "docker-compose.localserver.yml" up -d
        }
    }
    else {
        if (-not $containerExists) {
            docker compose --env-file ".env.localserver" -f "docker-compose.localserver.yml" up -d
        }
        elseif (-not $isRunning) {
            docker start $containerName
        }
        else {
            Write-Host "Container already running: $containerName"
        }
    }

    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose command failed."
    }

    $deadline = (Get-Date).AddSeconds(45)
    do {
        try {
            $resp = Invoke-RestMethod "http://127.0.0.1:8765/api/health" -TimeoutSec 3
            if ($resp.ok) {
                $stateText = if ($Action -eq "restart") { "Service restarted" } else { "Service started" }
                Write-Host "${stateText}: $($resp.version)"
                Write-Host "Service URL: http://127.0.0.1:8765"
                exit 0
            }
        }
        catch {
        }
        Start-Sleep -Milliseconds 800
    } while ((Get-Date) -lt $deadline)

    throw "Service startup timed out."
}
catch {
    Write-Error $_
    exit 1
}
finally {
    Pop-Location
}
