Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
startupScript = scriptDir & "\run.bat"
appScript = scriptDir & "\desktop_app.py"

' 把工作目錄切到本機，避免因 UNC 工作目錄而失敗
shell.CurrentDirectory = "C:\"

' 先在背景確認 Docker 版服務已啟動完成
exitCode = shell.Run("cmd /c """ & startupScript & """", 0, True)
If exitCode <> 0 Then
  MsgBox "Docker 版服務啟動失敗，請先確認 Docker Desktop 已開啟。", 16, "出貨排程系統"
  WScript.Quit exitCode
End If

' 用 python.exe + windowStyle=1（正常顯示）啟動桌面殼
' desktop_app.py 會連到本機 Docker 服務，不再自行開本地 uvicorn
shell.Run """python"" """ & appScript & """", 1, False
