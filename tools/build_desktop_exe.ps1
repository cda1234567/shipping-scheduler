Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$distDir = Join-Path $root "dist"
$bundleDir = Join-Path $distDir "DispatchSchedulerDesktop"
$zipPath = Join-Path $distDir "DispatchSchedulerDesktop-win64.zip"
$desktopConfigExample = Join-Path $root "desktop_client.json.example"

Push-Location $root
try {
  py -3 -m pip install --disable-pip-version-check -r requirements.txt pyinstaller | Out-Host

  if (Test-Path $bundleDir) {
    Remove-Item $bundleDir -Recurse -Force
  }
  if (Test-Path (Join-Path $root "build")) {
    Remove-Item (Join-Path $root "build") -Recurse -Force
  }
  if (Test-Path $zipPath) {
    Remove-Item $zipPath -Force
  }

  py -3 -m PyInstaller --noconfirm --clean desktop_app.spec | Out-Host

  if (!(Test-Path $bundleDir)) {
    throw "找不到打包結果：$bundleDir"
  }

  if (Test-Path $desktopConfigExample) {
    Copy-Item $desktopConfigExample (Join-Path $bundleDir "desktop_client.json.example") -Force
  }

  Compress-Archive -Path (Join-Path $bundleDir "*") -DestinationPath $zipPath -Force

  Write-Host ""
  Write-Host "桌面版打包完成：" -ForegroundColor Green
  Write-Host "EXE 目錄: $bundleDir"
  Write-Host "ZIP 檔案: $zipPath"
}
finally {
  Pop-Location
}
