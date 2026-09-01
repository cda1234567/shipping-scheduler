Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:OlFolderDeletedItems = 3
$script:OlFolderOutbox = 4
$script:OlFolderDrafts = 16
$script:OlFolderConflicts = 19
$script:OlFolderSyncIssues = 20
$script:OlFolderLocalFailures = 21
$script:OlFolderServerFailures = 22
$script:OlFolderJunk = 23
$script:OlFolderRssFeeds = 25
$script:OlObjectClassMail = 43
$script:OlItemTypeMail = 0
$script:InternetMessageIdDasl = 'http://schemas.microsoft.com/mapi/proptag/0x1035001F'
$script:RecommendedExcludedDefaultFolders = @(
  $script:OlFolderDeletedItems,
  $script:OlFolderOutbox,
  $script:OlFolderDrafts,
  $script:OlFolderConflicts,
  $script:OlFolderSyncIssues,
  $script:OlFolderLocalFailures,
  $script:OlFolderServerFailures,
  $script:OlFolderJunk,
  $script:OlFolderRssFeeds
)
$script:RecommendedExcludedNamePatterns = @(
  'deleted items',
  '刪除的郵件',
  '垃圾郵件',
  'junk email',
  'rss feeds',
  'rss 摘要',
  'rss',
  'sync issues',
  '同步問題',
  'conflicts',
  '衝突',
  'local failures',
  '本機失敗',
  'server failures',
  '伺服器失敗',
  'conversation action settings',
  '對話動作設定',
  'quick step settings',
  '快速步驟設定',
  'search folders',
  '搜尋資料夾',
  'outbox',
  '寄件匣',
  'drafts',
  '草稿'
)

function Normalize-OutlookDuplicateText {
  param(
    [AllowNull()]
    [object]$Value
  )

  if ($null -eq $Value) {
    return ''
  }

  return ([string]$Value).Trim().ToLowerInvariant()
}

function Open-OutlookSession {
  try {
    $app = [Runtime.InteropServices.Marshal]::GetActiveObject('Outlook.Application')
  }
  catch {
    $app = New-Object -ComObject Outlook.Application
  }

  $namespace = $app.GetNamespace('MAPI')
  $namespace.Logon($null, $null, $false, $false) | Out-Null

  return [PSCustomObject]@{
    App = $app
    Namespace = $namespace
  }
}

function Close-OutlookSession {
  param(
    [AllowNull()]
    [object]$Session
  )

  if ($null -eq $Session) {
    return
  }

  foreach ($obj in @($Session.Namespace, $Session.App)) {
    if ($null -ne $obj -and [Runtime.InteropServices.Marshal]::IsComObject($obj)) {
      [void][Runtime.InteropServices.Marshal]::ReleaseComObject($obj)
    }
  }

  [gc]::Collect()
  [gc]::WaitForPendingFinalizers()
}

function Get-OutlookMailboxNames {
  $session = $null

  try {
    $session = Open-OutlookSession
    $names = New-Object System.Collections.Generic.List[string]
    foreach ($folder in $session.Namespace.Folders) {
      $names.Add([string]$folder.Name)
    }

    return $names.ToArray()
  }
  finally {
    Close-OutlookSession -Session $session
  }
}

function Resolve-MailboxRoot {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Namespace,
    [string]$Mailbox
  )

  if ([string]::IsNullOrWhiteSpace($Mailbox)) {
    $defaultStore = $Namespace.DefaultStore
    if ($null -eq $defaultStore) {
      throw '找不到 Outlook 預設信箱。'
    }

    return $defaultStore.GetRootFolder()
  }

  try {
    $root = $Namespace.Folders.Item($Mailbox)
  }
  catch {
    $root = $null
  }

  if ($null -eq $root) {
    throw "找不到信箱：$Mailbox"
  }

  return $root
}

function Resolve-OutlookFolderInternal {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Namespace,
    [string]$Mailbox,
    [Parameter(Mandatory = $true)]
    [string]$FolderPath
  )

  $current = Resolve-MailboxRoot -Namespace $Namespace -Mailbox $Mailbox
  $segments = @(
    $FolderPath.Split('\', [System.StringSplitOptions]::RemoveEmptyEntries) |
      ForEach-Object { $_.Trim() } |
      Where-Object { $_ }
  )

  if ($segments.Count -eq 0) {
    return $current
  }

  foreach ($segment in $segments) {
    try {
      $next = $current.Folders.Item($segment)
    }
    catch {
      $next = $null
    }

    if ($null -eq $next) {
      throw "找不到資料夾：$FolderPath（卡在 '$segment'）"
    }

    $current = $next
  }

  return $current
}

function Get-OutlookFoldersToScan {
  param(
    [Parameter(Mandatory = $true)]
    [object]$RootFolder,
    [switch]$IncludeSubfolders
  )

  $result = New-Object System.Collections.Generic.List[object]
  $result.Add($RootFolder)

  if (-not $IncludeSubfolders) {
    return $result.ToArray()
  }

  $stack = New-Object System.Collections.Generic.Stack[object]
  foreach ($child in $RootFolder.Folders) {
    $stack.Push($child)
  }

  while ($stack.Count -gt 0) {
    $folder = $stack.Pop()
    $result.Add($folder)

    foreach ($child in $folder.Folders) {
      $stack.Push($child)
    }
  }

  return $result.ToArray()
}

function Get-OutlookFolderLeafName {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Folder
  )

  try {
    return [string]$Folder.Name
  }
  catch {
    $path = [string]$Folder.FolderPath
    if ([string]::IsNullOrWhiteSpace($path)) {
      return ''
    }

    return ($path -split '\\')[-1]
  }
}

function Get-OutlookRecommendedExcludedFolderMap {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Store
  )

  $excluded = @{}

  foreach ($folderType in $script:RecommendedExcludedDefaultFolders) {
    try {
      $folder = $Store.GetDefaultFolder($folderType)
      if ($null -ne $folder) {
        $excluded[[string]$folder.EntryID] = [string]$folder.FolderPath
      }
    }
    catch {
      continue
    }
  }

  return $excluded
}

function Get-OutlookRecommendedFolderSkipReason {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Folder,
    [Parameter(Mandatory = $true)]
    [hashtable]$ExcludedEntryIds
  )

  $entryId = [string]$Folder.EntryID
  if ($entryId -and $ExcludedEntryIds.ContainsKey($entryId)) {
    return '推薦模式略過系統資料夾'
  }

  $leafName = Normalize-OutlookDuplicateText (Get-OutlookFolderLeafName -Folder $Folder)
  foreach ($pattern in $script:RecommendedExcludedNamePatterns) {
    if ($leafName -eq $pattern) {
      return '推薦模式略過高噪音資料夾'
    }
  }

  return ''
}

function New-OutlookSkippedFolderResult {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Folder,
    [Parameter(Mandatory = $true)]
    [string]$Reason
  )

  return [PSCustomObject]@{
    FolderPath = [string]$Folder.FolderPath
    MailCount = 0
    DuplicateCount = 0
    Duplicates = @()
    Skipped = $true
    SkipReason = $Reason
  }
}

function Get-OutlookInternetMessageId {
  param(
    [Parameter(Mandatory = $true)]
    [object]$MailItem
  )

  try {
    return Normalize-OutlookDuplicateText $MailItem.PropertyAccessor.GetProperty($script:InternetMessageIdDasl)
  }
  catch {
    return ''
  }
}

function Get-OutlookMailDateTime {
  param(
    [Parameter(Mandatory = $true)]
    [object]$MailItem,
    [Parameter(Mandatory = $true)]
    [string]$PropertyName
  )

  try {
    return [datetime]$MailItem.$PropertyName
  }
  catch {
    return $null
  }
}

function Get-OutlookMailSize {
  param(
    [Parameter(Mandatory = $true)]
    [object]$MailItem
  )

  try {
    return [int]$MailItem.Size
  }
  catch {
    return 0
  }
}

function Get-OutlookMailDuplicateKey {
  param(
    [Parameter(Mandatory = $true)]
    [object]$MailItem
  )

  $messageId = Get-OutlookInternetMessageId -MailItem $MailItem
  if ($messageId) {
    return "message-id::$messageId"
  }

  $sender = Normalize-OutlookDuplicateText $MailItem.SenderEmailAddress
  $subject = Normalize-OutlookDuplicateText $MailItem.Subject
  $sentOn = Get-OutlookMailDateTime -MailItem $MailItem -PropertyName 'SentOn'
  $size = Get-OutlookMailSize -MailItem $MailItem

  $sentKey = if ($null -eq $sentOn) {
    ''
  }
  else {
    $sentOn.ToString('yyyy-MM-ddTHH:mm:ss')
  }

  return "fallback::$sender|$subject|$sentKey|$size"
}

function New-OutlookMailRecord {
  param(
    [Parameter(Mandatory = $true)]
    [object]$MailItem,
    [Parameter(Mandatory = $true)]
    [object]$Folder,
    [Parameter(Mandatory = $true)]
    [string]$DuplicateKey
  )

  return [PSCustomObject]@{
    FolderPath = [string]$Folder.FolderPath
    StoreId = [string]$Folder.Store.StoreID
    EntryId = [string]$MailItem.EntryID
    Subject = [string]$MailItem.Subject
    Sender = [string]$MailItem.SenderEmailAddress
    ReceivedTime = Get-OutlookMailDateTime -MailItem $MailItem -PropertyName 'ReceivedTime'
    SentOn = Get-OutlookMailDateTime -MailItem $MailItem -PropertyName 'SentOn'
    Size = Get-OutlookMailSize -MailItem $MailItem
    DuplicateKey = $DuplicateKey
    MatchType = if ($DuplicateKey.StartsWith('message-id::')) { 'InternetMessageId' } else { '寄件者+主旨+寄出時間+大小' }
  }
}

function Test-OutlookMailFolder {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Folder
  )

  try {
    return ([int]$Folder.DefaultItemType -eq $script:OlItemTypeMail)
  }
  catch {
    return $false
  }
}

function Find-OutlookDuplicatesInFolder {
  param(
    [Parameter(Mandatory = $true)]
    [object]$Folder,
    [switch]$KeepNewest,
    [scriptblock]$OnItemProgress,
    [int]$ProgressInterval = 500
  )

  if (-not (Test-OutlookMailFolder -Folder $Folder)) {
    return [PSCustomObject]@{
      FolderPath = [string]$Folder.FolderPath
      MailCount = 0
      DuplicateCount = 0
      Duplicates = @()
      Skipped = $true
      SkipReason = '非郵件資料夾'
    }
  }

  $items = $Folder.Items
  try {
    $items.Sort('[ReceivedTime]', [bool]$KeepNewest)
  }
  catch {
    # 某些郵件資料夾可能不支援這個排序欄位，退回原始順序掃描。
  }
  $itemCount = $items.Count

  $mailCount = 0
  $seen = @{}
  $duplicates = New-Object System.Collections.Generic.List[object]

  for ($index = 1; $index -le $itemCount; $index++) {
    $shouldReportProgress = $OnItemProgress -and (($index % $ProgressInterval) -eq 0)
    $item = $items.Item($index)
    if ($null -eq $item) {
      if ($shouldReportProgress) {
        & $OnItemProgress ([PSCustomObject]@{
          FolderPath = [string]$Folder.FolderPath
          ProcessedItemCount = $index
          TotalItemCount = $itemCount
          MailCount = $mailCount
          DuplicateCount = $duplicates.Count
        })
      }
      continue
    }

    if ([int]$item.Class -ne $script:OlObjectClassMail) {
      if ($shouldReportProgress) {
        & $OnItemProgress ([PSCustomObject]@{
          FolderPath = [string]$Folder.FolderPath
          ProcessedItemCount = $index
          TotalItemCount = $itemCount
          MailCount = $mailCount
          DuplicateCount = $duplicates.Count
        })
      }
      continue
    }

    $mailCount++
    $duplicateKey = Get-OutlookMailDuplicateKey -MailItem $item
    if ([string]::IsNullOrWhiteSpace($duplicateKey)) {
      if ($shouldReportProgress) {
        & $OnItemProgress ([PSCustomObject]@{
          FolderPath = [string]$Folder.FolderPath
          ProcessedItemCount = $index
          TotalItemCount = $itemCount
          MailCount = $mailCount
          DuplicateCount = $duplicates.Count
        })
      }
      continue
    }

    $record = New-OutlookMailRecord -MailItem $item -Folder $Folder -DuplicateKey $duplicateKey

    if ($seen.ContainsKey($duplicateKey)) {
      $kept = $seen[$duplicateKey]
      $duplicates.Add([PSCustomObject]@{
        FolderPath = $record.FolderPath
        StoreId = $record.StoreId
        EntryId = $record.EntryId
        Subject = $record.Subject
        Sender = $record.Sender
        ReceivedTime = $record.ReceivedTime
        SentOn = $record.SentOn
        Size = $record.Size
        DuplicateKey = $record.DuplicateKey
        MatchType = $record.MatchType
        KeptEntryId = $kept.EntryId
        KeptSubject = $kept.Subject
        KeptReceivedTime = $kept.ReceivedTime
      })
      continue
    }

    $seen[$duplicateKey] = $record

    if ($shouldReportProgress) {
      & $OnItemProgress ([PSCustomObject]@{
        FolderPath = [string]$Folder.FolderPath
        ProcessedItemCount = $index
        TotalItemCount = $itemCount
        MailCount = $mailCount
        DuplicateCount = $duplicates.Count
      })
    }
  }

  if ($OnItemProgress) {
    & $OnItemProgress ([PSCustomObject]@{
      FolderPath = [string]$Folder.FolderPath
      ProcessedItemCount = $itemCount
      TotalItemCount = $itemCount
      MailCount = $mailCount
      DuplicateCount = $duplicates.Count
    })
  }

  return [PSCustomObject]@{
    FolderPath = [string]$Folder.FolderPath
    MailCount = $mailCount
    DuplicateCount = $duplicates.Count
    Duplicates = $duplicates.ToArray()
    Skipped = $false
    SkipReason = ''
  }
}

function Invoke-OutlookDuplicateScan {
  [CmdletBinding()]
  param(
    [string]$Mailbox,
    [string]$FolderPath = 'Inbox',
    [switch]$AllFolders,
    [switch]$IncludeSubfolders,
    [switch]$KeepNewest,
    [switch]$UseRecommendedExclusions,
    [scriptblock]$OnFolderStarted,
    [scriptblock]$OnFolderScanned,
    [scriptblock]$OnItemProgress
  )

  $session = $null

  try {
    $session = Open-OutlookSession
    if ($AllFolders) {
      $rootFolder = Resolve-MailboxRoot -Namespace $session.Namespace -Mailbox $Mailbox
      $folders = Get-OutlookFoldersToScan -RootFolder $rootFolder -IncludeSubfolders:$true
    }
    else {
      $rootFolder = Resolve-OutlookFolderInternal -Namespace $session.Namespace -Mailbox $Mailbox -FolderPath $FolderPath
      $folders = Get-OutlookFoldersToScan -RootFolder $rootFolder -IncludeSubfolders:$IncludeSubfolders
    }

    $recommendedExcludedEntryIds = if ($UseRecommendedExclusions) {
      Get-OutlookRecommendedExcludedFolderMap -Store $rootFolder.Store
    }
    else {
      @{}
    }

    $allDuplicates = New-Object System.Collections.Generic.List[object]
    $folderResults = New-Object System.Collections.Generic.List[object]
    $totalMailCount = 0
    $skippedFolderCount = 0
    $excludedFolderCount = 0
    $nonMailFolderCount = 0
    $rootEntryId = [string]$rootFolder.EntryID

    foreach ($folder in $folders) {
      if ($OnFolderStarted) {
        & $OnFolderStarted ([PSCustomObject]@{
          FolderPath = [string]$folder.FolderPath
        })
      }

      $skipReason = ''
      if ($UseRecommendedExclusions) {
        $skipReason = Get-OutlookRecommendedFolderSkipReason -Folder $folder -ExcludedEntryIds $recommendedExcludedEntryIds
        if ((-not $AllFolders) -and ([string]$folder.EntryID -eq $rootEntryId)) {
          $skipReason = ''
        }
      }

      if ($skipReason) {
        $result = New-OutlookSkippedFolderResult -Folder $folder -Reason $skipReason
        $excludedFolderCount++
      }
      else {
        $result = Find-OutlookDuplicatesInFolder -Folder $folder -KeepNewest:$KeepNewest -OnItemProgress $OnItemProgress
        if ($result.Skipped -and $result.SkipReason -eq '非郵件資料夾') {
          $nonMailFolderCount++
        }
      }

      $totalMailCount += $result.MailCount
      $folderResults.Add($result)
      if ($result.Skipped) {
        $skippedFolderCount++
      }

      foreach ($duplicate in $result.Duplicates) {
        $allDuplicates.Add($duplicate)
      }

      if ($OnFolderScanned) {
        & $OnFolderScanned $result
      }
    }

    return [PSCustomObject]@{
      Mailbox = if ([string]::IsNullOrWhiteSpace($Mailbox)) { '' } else { $Mailbox }
      RootFolderPath = [string]$rootFolder.FolderPath
      AllFolders = [bool]$AllFolders
      UseRecommendedExclusions = [bool]$UseRecommendedExclusions
      FolderCount = $folders.Count
      SkippedFolderCount = $skippedFolderCount
      ExcludedFolderCount = $excludedFolderCount
      NonMailFolderCount = $nonMailFolderCount
      ScannedFolderCount = $folders.Count - $skippedFolderCount
      TotalMailCount = $totalMailCount
      DuplicateCount = $allDuplicates.Count
      FolderResults = $folderResults.ToArray()
      Duplicates = $allDuplicates.ToArray()
    }
  }
  finally {
    Close-OutlookSession -Session $session
  }
}

function Export-OutlookDuplicateReport {
  param(
    [Parameter(Mandatory = $true)]
    [object[]]$Duplicates,
    [Parameter(Mandatory = $true)]
    [string]$ReportPath
  )

  $resolvedPath = [System.IO.Path]::GetFullPath($ReportPath)
  $directory = Split-Path -Parent $resolvedPath
  if (-not [string]::IsNullOrWhiteSpace($directory) -and -not (Test-Path $directory)) {
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
  }

  $Duplicates |
    Select-Object FolderPath, Subject, Sender, ReceivedTime, SentOn, Size, MatchType, DuplicateKey, EntryId, KeptSubject, KeptReceivedTime |
    Export-Csv -Path $resolvedPath -NoTypeInformation -Encoding UTF8

  return $resolvedPath
}

function Get-OutlookDuplicateTargetLabel {
  param(
    [Parameter(Mandatory = $true)]
    [object]$DuplicateRecord
  )

  return "{0} ({1})" -f $DuplicateRecord.Subject, $DuplicateRecord.FolderPath
}

function Remove-OutlookDuplicateRecords {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory = $true)]
    [object[]]$DuplicateRecords,
    [switch]$PermanentDelete,
    [scriptblock]$OnItemProcessed
  )

  $session = $null

  try {
    $session = Open-OutlookSession
    $processed = 0
    $errors = 0

    foreach ($duplicate in $DuplicateRecords) {
      try {
        $item = $session.Namespace.GetItemFromID($duplicate.EntryId, $duplicate.StoreId)
        if ($null -eq $item) {
          $errors++
          if ($OnItemProcessed) {
            & $OnItemProcessed ([PSCustomObject]@{
              Success = $false
              Subject = $duplicate.Subject
              FolderPath = $duplicate.FolderPath
              Message = '找不到信件，可能已被移動或刪除。'
            })
          }
          continue
        }

        if ($PermanentDelete) {
          $item.Delete()
        }
        else {
          $deletedItemsFolder = $item.Parent.Store.GetDefaultFolder($script:OlFolderDeletedItems)
          if ([string]$item.Parent.FolderPath -eq [string]$deletedItemsFolder.FolderPath) {
            $item.Delete()
          }
          else {
            [void]$item.Move($deletedItemsFolder)
          }
        }

        $processed++
        if ($OnItemProcessed) {
          & $OnItemProcessed ([PSCustomObject]@{
            Success = $true
            Subject = $duplicate.Subject
            FolderPath = $duplicate.FolderPath
            Message = if ($PermanentDelete) { '已永久刪除' } else { '已移到垃圾桶' }
          })
        }
      }
      catch {
        $errors++
        if ($OnItemProcessed) {
          & $OnItemProcessed ([PSCustomObject]@{
            Success = $false
            Subject = $duplicate.Subject
            FolderPath = $duplicate.FolderPath
            Message = $_.Exception.Message
          })
        }
      }
    }

    return [PSCustomObject]@{
      Processed = $processed
      Errors = $errors
    }
  }
  finally {
    Close-OutlookSession -Session $session
  }
}

Export-ModuleMember -Function @(
  'Get-OutlookMailboxNames',
  'Invoke-OutlookDuplicateScan',
  'Export-OutlookDuplicateReport',
  'Get-OutlookDuplicateTargetLabel',
  'Remove-OutlookDuplicateRecords'
)
