[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = 'High')]
param(
  [string]$Mailbox,
  [string]$FolderPath = 'Inbox',
  [switch]$AllFolders,
  [switch]$IncludeSubfolders,
  [switch]$DisableRecommendedExclusions,
  [switch]$Apply,
  [switch]$PermanentDelete,
  [switch]$KeepNewest,
  [string]$ReportPath,
  [switch]$ListMailboxes
)

<#
.SYNOPSIS
整理 Outlook 郵件並移除重複信件。

.DESCRIPTION
預設只掃描不修改任何信件。確認結果後，加上 -Apply 才會執行動作。
預設行為是把重複信件移到 Outlook 的垃圾桶；若再加上 -PermanentDelete，則改為永久刪除。
預設也會啟用推薦排除規則，自動略過垃圾桶、垃圾郵件、RSS、同步問題、草稿、寄件匣等高噪音資料夾。

重複判定規則：
1. 優先使用 InternetMessageId
2. 若取不到，退回用「寄件者 + 主旨 + 寄出時間 + 大小」比對

預設只保留每個資料夾內第一封信：
- 不加 -KeepNewest：保留較早收到的那封
- 加上 -KeepNewest：保留較新的那封

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1

先掃描預設信箱的 Inbox，預覽重複信件，不做任何修改。

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1 -FolderPath "Inbox\\報價" -IncludeSubfolders -ReportPath .\duplicates.csv

掃描指定資料夾與子資料夾，並輸出 CSV 報表。

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1 -FolderPath "Inbox" -Apply

把 Inbox 內的重複信件移到 Outlook 垃圾桶。

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1 -Mailbox "你的信箱名稱" -FolderPath "Inbox\\通知" -Apply -KeepNewest

掃描指定信箱的資料夾，只保留較新的那封重複信件，其餘移到垃圾桶。

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1 -Mailbox "你的信箱名稱" -AllFolders

掃描整個信箱的全部資料夾並預覽重複信件。

.EXAMPLE
powershell -ExecutionPolicy Bypass -File .\tools\remove_outlook_duplicates.ps1 -Mailbox "你的信箱名稱" -AllFolders -DisableRecommendedExclusions

連高噪音系統資料夾也一併掃描。
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$modulePath = Join-Path $PSScriptRoot 'OutlookDuplicateTools.psm1'
Import-Module $modulePath -Force

function Write-DuplicatePreview {
  param(
    [Parameter(Mandatory = $true)]
    [object[]]$Duplicates
  )

  if ($Duplicates.Count -eq 0) {
    Write-Host '沒有找到重複信件。' -ForegroundColor Green
    return
  }

  Write-Host ("找到 {0} 封重複信件，以下先顯示前 20 筆：" -f $Duplicates.Count) -ForegroundColor Yellow
  $Duplicates |
    Select-Object -First 20 FolderPath, Subject, Sender, ReceivedTime, MatchType |
    Format-Table -AutoSize |
    Out-Host
}

if ($ListMailboxes) {
  Write-Host '可用信箱：' -ForegroundColor Cyan
  foreach ($name in (Get-OutlookMailboxNames)) {
    Write-Host (" - {0}" -f $name)
  }
  return
}

$useRecommendedExclusions = -not $DisableRecommendedExclusions

$scanResult = Invoke-OutlookDuplicateScan -Mailbox $Mailbox -FolderPath $FolderPath -AllFolders:$AllFolders -IncludeSubfolders:$IncludeSubfolders -KeepNewest:$KeepNewest -UseRecommendedExclusions:$useRecommendedExclusions -OnFolderScanned {
  param($result)
  if ($result.Skipped) {
    Write-Host ("[{0}] 已略過：{1}" -f $result.FolderPath, $result.SkipReason) -ForegroundColor DarkYellow
  }
  else {
    Write-Host ("[{0}] 掃描 {1} 封信，找到 {2} 封重複" -f $result.FolderPath, $result.MailCount, $result.DuplicateCount)
  }
}

if ($AllFolders) {
  Write-Host ("開始掃描整個信箱：{0}" -f $scanResult.RootFolderPath) -ForegroundColor Cyan
  Write-Host ("包含全部資料夾，共 {0} 個資料夾" -f $scanResult.FolderCount) -ForegroundColor Cyan
}
else {
  Write-Host ("開始掃描：{0}" -f $scanResult.RootFolderPath) -ForegroundColor Cyan
}
if ((-not $AllFolders) -and $IncludeSubfolders) {
  Write-Host ("包含子資料夾，共 {0} 個資料夾" -f $scanResult.FolderCount) -ForegroundColor Cyan
}

$duplicates = @($scanResult.Duplicates)
Write-Host ''
Write-Host ("總計掃描 {0} 封郵件，找到 {1} 封重複信件。" -f $scanResult.TotalMailCount, $duplicates.Count) -ForegroundColor Yellow
if ($scanResult.SkippedFolderCount -gt 0) {
  Write-Host ("已自動略過 {0} 個不需要掃描的資料夾。" -f $scanResult.SkippedFolderCount) -ForegroundColor DarkYellow
}
if ($scanResult.ExcludedFolderCount -gt 0) {
  Write-Host ("推薦模式已額外略過 {0} 個高噪音資料夾。" -f $scanResult.ExcludedFolderCount) -ForegroundColor DarkYellow
}
if ($scanResult.NonMailFolderCount -gt 0) {
  Write-Host ("其中有 {0} 個是非郵件資料夾。" -f $scanResult.NonMailFolderCount) -ForegroundColor DarkYellow
}

Write-DuplicatePreview -Duplicates $duplicates

if ($ReportPath) {
  $resolvedReport = Export-OutlookDuplicateReport -Duplicates $duplicates -ReportPath $ReportPath
  Write-Host "報表已輸出：$resolvedReport" -ForegroundColor Cyan
}

if (-not $Apply) {
  Write-Host ''
  Write-Host '目前是預覽模式，尚未刪除任何信件。' -ForegroundColor Green
  Write-Host '確認結果沒問題後，請加上 -Apply 再執行一次。' -ForegroundColor Green
  return
}

if ($duplicates.Count -eq 0) {
  return
}

$approvedDuplicates = New-Object System.Collections.Generic.List[object]
$actionName = if ($PermanentDelete) { '永久刪除' } else { '移到垃圾桶' }

foreach ($duplicate in $duplicates) {
  $targetLabel = Get-OutlookDuplicateTargetLabel -DuplicateRecord $duplicate
  if ($PSCmdlet.ShouldProcess($targetLabel, $actionName)) {
    $approvedDuplicates.Add($duplicate)
  }
}

if ($approvedDuplicates.Count -eq 0) {
  Write-Host '沒有任何信件被核准處理。' -ForegroundColor Yellow
  return
}

Write-Host ''
Write-Host '開始處理重複信件...' -ForegroundColor Yellow
$removeResult = Remove-OutlookDuplicateRecords -DuplicateRecords $approvedDuplicates.ToArray() -PermanentDelete:$PermanentDelete

Write-Host ''
Write-Host ("已處理 {0} 封，失敗 {1} 封。" -f $removeResult.Processed, $removeResult.Errors) -ForegroundColor Yellow
if ($PermanentDelete) {
  Write-Host '本次使用的是永久刪除。' -ForegroundColor Yellow
}
else {
  Write-Host '本次已把重複信件移到 Outlook 垃圾桶。' -ForegroundColor Yellow
}
