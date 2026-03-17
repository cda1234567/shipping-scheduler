Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
appScript = scriptDir & "\desktop_app.py"

' 把工作目錄切到本機，避免因 UNC 工作目錄而失敗
shell.CurrentDirectory = "C:\"

' 用 python.exe + windowStyle=1（正常顯示）啟動
' desktop_app.py 內部會在 webview 視窗建立後自動隱藏 console
shell.Run """python"" """ & appScript & """", 1, False
