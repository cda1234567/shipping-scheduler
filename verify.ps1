$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Get-NodeExecutable {
  $nodeCmd = Get-Command node -ErrorAction SilentlyContinue
  if ($nodeCmd) {
    return $nodeCmd.Source
  }

  $baseDir = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Packages\OpenJS.NodeJS.LTS_Microsoft.Winget.Source_8wekyb3d8bbwe'
  if (Test-Path $baseDir) {
    $nodeExe = Get-ChildItem $baseDir -Directory | ForEach-Object {
      Join-Path $_.FullName 'node.exe'
    } | Where-Object { Test-Path $_ } | Select-Object -First 1

    if ($nodeExe) {
      return $nodeExe
    }
  }

  throw 'Node.js not found. Please install Node.js LTS first.'
}

Write-Host '[1/2] Running Python tests...'
python -u -m unittest discover -s tests -p test_*.py
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

$nodeExe = Get-NodeExecutable

Write-Host '[2/2] Checking JavaScript syntax...'
& $nodeExe (Join-Path $PSScriptRoot 'tools\check_js.mjs')
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

Write-Host 'All checks passed.'
