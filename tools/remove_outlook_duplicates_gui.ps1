Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ([System.Threading.Thread]::CurrentThread.ApartmentState -ne 'STA') {
  $arguments = @(
    '-NoProfile',
    '-STA',
    '-ExecutionPolicy', 'Bypass',
    '-File', ('"{0}"' -f $PSCommandPath)
  )
  Start-Process -FilePath 'powershell.exe' -ArgumentList $arguments | Out-Null
  return
}

Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing

$modulePath = Join-Path $PSScriptRoot 'OutlookDuplicateTools.psm1'
Import-Module $modulePath -Force

[System.Windows.Forms.Application]::EnableVisualStyles()

$script:LastScanResult = $null
$script:IsBusy = $false

function Add-Log {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Message
  )

  $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $txtLog.AppendText("[${timestamp}] $Message`r`n")
  $txtLog.SelectionStart = $txtLog.TextLength
  $txtLog.ScrollToCaret()
}

function Update-FolderInputState {
  if ($null -eq $txtFolderPath) {
    return
  }

  $folderEnabled = (-not $script:IsBusy) -and (-not $chkAllFolders.Checked)
  $txtFolderPath.Enabled = $folderEnabled
  $chkIncludeSubfolders.Enabled = (-not $script:IsBusy) -and (-not $chkAllFolders.Checked)
}

function Update-PreviewStatus {
  param(
    [Parameter(Mandatory = $true)]
    [string]$Message
  )

  $lblSummary.Text = $Message
  [System.Windows.Forms.Application]::DoEvents()
}

function Set-BusyState {
  param(
    [bool]$Busy
  )

  $script:IsBusy = $Busy
  $btnRefreshMailboxes.Enabled = -not $Busy
  $btnBrowseReport.Enabled = -not $Busy
  $btnPreview.Enabled = -not $Busy
  $btnApply.Enabled = (-not $Busy) -and ($null -ne $script:LastScanResult) -and ($script:LastScanResult.DuplicateCount -gt 0)
  $btnClear.Enabled = -not $Busy
  $cmbMailbox.Enabled = -not $Busy
  $chkAllFolders.Enabled = -not $Busy
  $chkRecommendedMode.Enabled = -not $Busy
  $chkKeepNewest.Enabled = -not $Busy
  $chkPermanentDelete.Enabled = -not $Busy
  $txtReportPath.Enabled = -not $Busy
  Update-FolderInputState
  $form.UseWaitCursor = $Busy
  [System.Windows.Forms.Application]::DoEvents()
}

function Get-SelectedMailbox {
  $selected = [string]$cmbMailbox.SelectedItem
  if ($selected -eq '(預設信箱)') {
    return ''
  }

  return $selected
}

function Clear-Results {
  $script:LastScanResult = $null
  $gridResults.DataSource = $null
  $lblSummary.Text = '尚未掃描'
  $btnApply.Enabled = $false
}

function Fill-ResultGrid {
  param(
    [Parameter(Mandatory = $true)]
    [object[]]$Duplicates
  )

  $table = New-Object System.Data.DataTable
  [void]$table.Columns.Add('資料夾')
  [void]$table.Columns.Add('主旨')
  [void]$table.Columns.Add('寄件者')
  [void]$table.Columns.Add('收到時間')
  [void]$table.Columns.Add('判定方式')

  foreach ($duplicate in $Duplicates) {
    $row = $table.NewRow()
    $row['資料夾'] = [string]$duplicate.FolderPath
    $row['主旨'] = [string]$duplicate.Subject
    $row['寄件者'] = [string]$duplicate.Sender
    $row['收到時間'] = if ($null -eq $duplicate.ReceivedTime) { '' } else { ([datetime]$duplicate.ReceivedTime).ToString('yyyy-MM-dd HH:mm:ss') }
    $row['判定方式'] = [string]$duplicate.MatchType
    [void]$table.Rows.Add($row)
  }

  $gridResults.DataSource = $table
  $gridResults.AutoResizeColumns([System.Windows.Forms.DataGridViewAutoSizeColumnsMode]::DisplayedCells)
}

function Load-Mailboxes {
  Set-BusyState -Busy $true
  try {
    Add-Log '正在載入 Outlook 信箱清單...'
    $mailboxes = Get-OutlookMailboxNames
    $cmbMailbox.Items.Clear()
    [void]$cmbMailbox.Items.Add('(預設信箱)')
    foreach ($mailbox in $mailboxes) {
      [void]$cmbMailbox.Items.Add($mailbox)
    }
    $cmbMailbox.SelectedIndex = 0
    Add-Log ("已載入 {0} 個信箱。" -f $mailboxes.Count)
  }
  catch {
    [System.Windows.Forms.MessageBox]::Show("載入信箱失敗：$($_.Exception.Message)", 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
    Add-Log ("載入信箱失敗：{0}" -f $_.Exception.Message)
  }
  finally {
    Set-BusyState -Busy $false
  }
}

function Run-Preview {
  $allFolders = $chkAllFolders.Checked
  $useRecommendedExclusions = $chkRecommendedMode.Checked
  $folderPath = $txtFolderPath.Text.Trim()
  if ((-not $allFolders) -and [string]::IsNullOrWhiteSpace($folderPath)) {
    [System.Windows.Forms.MessageBox]::Show('請先輸入要掃描的 Outlook 資料夾路徑，例如 Inbox 或 Inbox\報價。', 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Warning) | Out-Null
    return
  }

  Clear-Results
  Set-BusyState -Busy $true
  try {
    if ($allFolders) {
      Add-Log '開始掃描整個信箱的全部資料夾。'
      Update-PreviewStatus -Message '正在準備掃描整個信箱全部資料夾...'
    }
    else {
      Add-Log ("開始掃描資料夾：{0}" -f $folderPath)
      Update-PreviewStatus -Message ("正在準備掃描資料夾：{0}" -f $folderPath)
    }

    if ($useRecommendedExclusions) {
      Add-Log '推薦模式已啟用：會略過垃圾桶、垃圾郵件、RSS、同步問題、草稿、寄件匣等高噪音資料夾。'
    }

    $scanResult = Invoke-OutlookDuplicateScan `
      -Mailbox (Get-SelectedMailbox) `
      -FolderPath $folderPath `
      -AllFolders:$allFolders `
      -IncludeSubfolders:$chkIncludeSubfolders.Checked `
      -KeepNewest:$chkKeepNewest.Checked `
      -UseRecommendedExclusions:$useRecommendedExclusions `
      -OnFolderStarted {
        param($info)
        Add-Log ("正在掃描：{0}" -f $info.FolderPath)
        Update-PreviewStatus -Message ("正在掃描：{0}" -f $info.FolderPath)
      } `
      -OnItemProgress {
        param($progress)
        Update-PreviewStatus -Message ("掃描中：{0} | 已處理 {1}/{2} 筆 | 郵件 {3} | 重複 {4}" -f $progress.FolderPath, $progress.ProcessedItemCount, $progress.TotalItemCount, $progress.MailCount, $progress.DuplicateCount)
      }
    $script:LastScanResult = $scanResult
    Fill-ResultGrid -Duplicates @($scanResult.Duplicates)

    $lblSummary.Text = "已掃描 $($scanResult.TotalMailCount) 封郵件，找到 $($scanResult.DuplicateCount) 封重複，可掃描資料夾 $($scanResult.ScannedFolderCount)，略過 $($scanResult.SkippedFolderCount)"
    Add-Log ("掃描完成：共 {0} 封郵件，找到 {1} 封重複。" -f $scanResult.TotalMailCount, $scanResult.DuplicateCount)
    if ($scanResult.SkippedFolderCount -gt 0) {
      Add-Log ("已自動略過 {0} 個不需要掃描的資料夾。" -f $scanResult.SkippedFolderCount)
    }
    if ($scanResult.ExcludedFolderCount -gt 0) {
      Add-Log ("推薦模式額外略過 {0} 個高噪音資料夾。" -f $scanResult.ExcludedFolderCount)
    }
    if ($scanResult.NonMailFolderCount -gt 0) {
      Add-Log ("其中有 {0} 個是非郵件資料夾。" -f $scanResult.NonMailFolderCount)
    }

    $reportPath = $txtReportPath.Text.Trim()
    if (-not [string]::IsNullOrWhiteSpace($reportPath)) {
      $resolvedReport = Export-OutlookDuplicateReport -Duplicates @($scanResult.Duplicates) -ReportPath $reportPath
      Add-Log ("報表已輸出：{0}" -f $resolvedReport)
    }

    if ($scanResult.DuplicateCount -eq 0) {
      Add-Log '沒有找到重複信件。'
    }
  }
  catch {
    Clear-Results
    Add-Log ("掃描失敗：{0}" -f $_.Exception.Message)
    [System.Windows.Forms.MessageBox]::Show("掃描失敗：$($_.Exception.Message)", 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
  }
  finally {
    Set-BusyState -Busy $false
  }
}

function Run-Apply {
  if ($null -eq $script:LastScanResult -or $script:LastScanResult.DuplicateCount -le 0) {
    [System.Windows.Forms.MessageBox]::Show('請先做一次預覽掃描，確認有重複信件後再執行清理。', 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
    return
  }

  $duplicateCount = [int]$script:LastScanResult.DuplicateCount
  $modeText = if ($chkPermanentDelete.Checked) { '永久刪除' } else { '移到垃圾桶' }
  $confirmText = "即將處理 $duplicateCount 封重複信件，動作：$modeText。`r`n`r`n建議先確認預覽清單無誤，再繼續。"
  $confirmResult = [System.Windows.Forms.MessageBox]::Show($confirmText, '確認清理', [System.Windows.Forms.MessageBoxButtons]::OKCancel, [System.Windows.Forms.MessageBoxIcon]::Warning)
  if ($confirmResult -ne [System.Windows.Forms.DialogResult]::OK) {
    return
  }

  Set-BusyState -Busy $true
  try {
    Add-Log ("開始清理 {0} 封重複信件，模式：{1}" -f $duplicateCount, $modeText)
    $removeResult = Remove-OutlookDuplicateRecords -DuplicateRecords @($script:LastScanResult.Duplicates) -PermanentDelete:$chkPermanentDelete.Checked
    Add-Log ("清理完成：成功 {0} 封，失敗 {1} 封。" -f $removeResult.Processed, $removeResult.Errors)
    $lblSummary.Text = "已完成清理：成功 $($removeResult.Processed) 封，失敗 $($removeResult.Errors) 封"
    [System.Windows.Forms.MessageBox]::Show("清理完成。`r`n成功：$($removeResult.Processed)`r`n失敗：$($removeResult.Errors)", 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Information) | Out-Null
    Clear-Results
  }
  catch {
    Add-Log ("清理失敗：{0}" -f $_.Exception.Message)
    [System.Windows.Forms.MessageBox]::Show("清理失敗：$($_.Exception.Message)", 'Outlook 去重工具', [System.Windows.Forms.MessageBoxButtons]::OK, [System.Windows.Forms.MessageBoxIcon]::Error) | Out-Null
  }
  finally {
    Set-BusyState -Busy $false
  }
}

$form = New-Object System.Windows.Forms.Form
$form.Text = 'Outlook 重複信件整理工具'
$form.StartPosition = 'CenterScreen'
$form.Size = New-Object System.Drawing.Size(1120, 780)
$form.MinimumSize = New-Object System.Drawing.Size(980, 680)

$panelTop = New-Object System.Windows.Forms.Panel
$panelTop.Dock = 'Top'
$panelTop.Height = 168
$panelTop.Padding = New-Object System.Windows.Forms.Padding(12)
$form.Controls.Add($panelTop)

$lblMailbox = New-Object System.Windows.Forms.Label
$lblMailbox.Text = '信箱'
$lblMailbox.Location = New-Object System.Drawing.Point(12, 16)
$lblMailbox.AutoSize = $true
$panelTop.Controls.Add($lblMailbox)

$cmbMailbox = New-Object System.Windows.Forms.ComboBox
$cmbMailbox.Location = New-Object System.Drawing.Point(88, 12)
$cmbMailbox.Size = New-Object System.Drawing.Size(360, 28)
$cmbMailbox.DropDownStyle = 'DropDownList'
$panelTop.Controls.Add($cmbMailbox)

$btnRefreshMailboxes = New-Object System.Windows.Forms.Button
$btnRefreshMailboxes.Text = '重新載入信箱'
$btnRefreshMailboxes.Location = New-Object System.Drawing.Point(462, 11)
$btnRefreshMailboxes.Size = New-Object System.Drawing.Size(120, 30)
$panelTop.Controls.Add($btnRefreshMailboxes)

$lblFolderPath = New-Object System.Windows.Forms.Label
$lblFolderPath.Text = '資料夾'
$lblFolderPath.Location = New-Object System.Drawing.Point(12, 54)
$lblFolderPath.AutoSize = $true
$panelTop.Controls.Add($lblFolderPath)

$txtFolderPath = New-Object System.Windows.Forms.TextBox
$txtFolderPath.Location = New-Object System.Drawing.Point(88, 50)
$txtFolderPath.Size = New-Object System.Drawing.Size(494, 27)
$txtFolderPath.Text = 'Inbox'
$panelTop.Controls.Add($txtFolderPath)

$chkIncludeSubfolders = New-Object System.Windows.Forms.CheckBox
$chkIncludeSubfolders.Text = '包含子資料夾'
$chkIncludeSubfolders.Location = New-Object System.Drawing.Point(600, 14)
$chkIncludeSubfolders.AutoSize = $true
$panelTop.Controls.Add($chkIncludeSubfolders)

$chkAllFolders = New-Object System.Windows.Forms.CheckBox
$chkAllFolders.Text = '整個信箱全部資料夾'
$chkAllFolders.Location = New-Object System.Drawing.Point(740, 14)
$chkAllFolders.AutoSize = $true
$panelTop.Controls.Add($chkAllFolders)

$chkRecommendedMode = New-Object System.Windows.Forms.CheckBox
$chkRecommendedMode.Text = '推薦模式（略過高噪音資料夾）'
$chkRecommendedMode.Location = New-Object System.Drawing.Point(740, 52)
$chkRecommendedMode.AutoSize = $true
$chkRecommendedMode.Checked = $true
$panelTop.Controls.Add($chkRecommendedMode)

$chkKeepNewest = New-Object System.Windows.Forms.CheckBox
$chkKeepNewest.Text = '保留較新的那封'
$chkKeepNewest.Location = New-Object System.Drawing.Point(600, 52)
$chkKeepNewest.AutoSize = $true
$panelTop.Controls.Add($chkKeepNewest)

$chkPermanentDelete = New-Object System.Windows.Forms.CheckBox
$chkPermanentDelete.Text = '永久刪除（危險）'
$chkPermanentDelete.Location = New-Object System.Drawing.Point(600, 88)
$chkPermanentDelete.AutoSize = $true
$panelTop.Controls.Add($chkPermanentDelete)

$lblReportPath = New-Object System.Windows.Forms.Label
$lblReportPath.Text = '報表'
$lblReportPath.Location = New-Object System.Drawing.Point(12, 92)
$lblReportPath.AutoSize = $true
$panelTop.Controls.Add($lblReportPath)

$txtReportPath = New-Object System.Windows.Forms.TextBox
$txtReportPath.Location = New-Object System.Drawing.Point(88, 88)
$txtReportPath.Size = New-Object System.Drawing.Size(494, 27)
$panelTop.Controls.Add($txtReportPath)

$btnBrowseReport = New-Object System.Windows.Forms.Button
$btnBrowseReport.Text = '選擇...'
$btnBrowseReport.Location = New-Object System.Drawing.Point(462, 124)
$btnBrowseReport.Size = New-Object System.Drawing.Size(92, 30)
$panelTop.Controls.Add($btnBrowseReport)

$btnPreview = New-Object System.Windows.Forms.Button
$btnPreview.Text = '預覽重複信件'
$btnPreview.Location = New-Object System.Drawing.Point(600, 124)
$btnPreview.Size = New-Object System.Drawing.Size(140, 32)
$panelTop.Controls.Add($btnPreview)

$btnApply = New-Object System.Windows.Forms.Button
$btnApply.Text = '開始清理'
$btnApply.Location = New-Object System.Drawing.Point(750, 124)
$btnApply.Size = New-Object System.Drawing.Size(120, 32)
$btnApply.Enabled = $false
$panelTop.Controls.Add($btnApply)

$btnClear = New-Object System.Windows.Forms.Button
$btnClear.Text = '清空結果'
$btnClear.Location = New-Object System.Drawing.Point(880, 124)
$btnClear.Size = New-Object System.Drawing.Size(100, 32)
$panelTop.Controls.Add($btnClear)

$lblHint = New-Object System.Windows.Forms.Label
$lblHint.Text = '建議保留推薦模式。要全掃時勾整個信箱全部資料夾，系統會自動略過垃圾桶、RSS、同步問題等高噪音資料夾。'
$lblHint.Location = New-Object System.Drawing.Point(12, 124)
$lblHint.Size = New-Object System.Drawing.Size(430, 34)
$panelTop.Controls.Add($lblHint)

$panelMain = New-Object System.Windows.Forms.SplitContainer
$panelMain.Dock = 'Fill'
$panelMain.Orientation = 'Horizontal'
$form.Controls.Add($panelMain)

$panelResults = New-Object System.Windows.Forms.Panel
$panelResults.Dock = 'Fill'
$panelResults.Padding = New-Object System.Windows.Forms.Padding(12, 8, 12, 12)
$panelMain.Panel1.Controls.Add($panelResults)

$lblSummary = New-Object System.Windows.Forms.Label
$lblSummary.Text = '尚未掃描'
$lblSummary.Dock = 'Top'
$lblSummary.Height = 28
$panelResults.Controls.Add($lblSummary)

$gridResults = New-Object System.Windows.Forms.DataGridView
$gridResults.Dock = 'Fill'
$gridResults.ReadOnly = $true
$gridResults.AllowUserToAddRows = $false
$gridResults.AllowUserToDeleteRows = $false
$gridResults.AllowUserToOrderColumns = $false
$gridResults.SelectionMode = 'FullRowSelect'
$gridResults.MultiSelect = $false
$gridResults.AutoSizeRowsMode = 'AllCells'
$gridResults.RowHeadersVisible = $false
$gridResults.DefaultCellStyle.WrapMode = 'False'
$panelResults.Controls.Add($gridResults)

$panelLog = New-Object System.Windows.Forms.Panel
$panelLog.Dock = 'Fill'
$panelLog.Padding = New-Object System.Windows.Forms.Padding(12, 8, 12, 12)
$panelMain.Panel2.Controls.Add($panelLog)

$lblLog = New-Object System.Windows.Forms.Label
$lblLog.Text = '執行紀錄'
$lblLog.Dock = 'Top'
$lblLog.Height = 24
$panelLog.Controls.Add($lblLog)

$txtLog = New-Object System.Windows.Forms.TextBox
$txtLog.Dock = 'Fill'
$txtLog.Multiline = $true
$txtLog.ScrollBars = 'Vertical'
$txtLog.ReadOnly = $true
$txtLog.Font = New-Object System.Drawing.Font('Consolas', 10)
$panelLog.Controls.Add($txtLog)

$saveDialog = New-Object System.Windows.Forms.SaveFileDialog
$saveDialog.Filter = 'CSV 檔案 (*.csv)|*.csv|所有檔案 (*.*)|*.*'
$saveDialog.Title = '選擇重複信件報表輸出路徑'
$saveDialog.FileName = 'outlook-duplicates.csv'

$btnRefreshMailboxes.Add_Click({ Load-Mailboxes })
$chkAllFolders.Add_CheckedChanged({
  Update-FolderInputState
  if ($chkAllFolders.Checked) {
    Add-Log '已切換成整個信箱全部資料夾模式。'
  }
})
$chkRecommendedMode.Add_CheckedChanged({
  if ($chkRecommendedMode.Checked) {
    Add-Log '已啟用推薦模式。'
  }
  else {
    Add-Log '已停用推薦模式，會連高噪音系統資料夾一起掃描。'
  }
})
$btnBrowseReport.Add_Click({
  if ($saveDialog.ShowDialog() -eq [System.Windows.Forms.DialogResult]::OK) {
    $txtReportPath.Text = $saveDialog.FileName
  }
})
$btnPreview.Add_Click({ Run-Preview })
$btnApply.Add_Click({ Run-Apply })
$btnClear.Add_Click({
  Clear-Results
  $txtLog.Clear()
  Add-Log '已清空畫面結果。'
})
$form.Add_Shown({
  $panelMain.Panel1MinSize = 260
  $panelMain.Panel2MinSize = 180
  $panelMain.SplitterDistance = [Math]::Max(260, $panelMain.Height - 192)
  Add-Log 'GUI 已啟動。'
  Add-Log '請先選擇信箱與資料夾；如果要全掃，勾選「整個信箱全部資料夾」。'
  Add-Log '推薦模式預設已開啟，會自動略過容易拖慢掃描的高噪音資料夾。'
  Update-FolderInputState
  Load-Mailboxes
})

[void]$form.ShowDialog()
