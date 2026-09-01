Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
appScript = scriptDir & "\desktop_app.py"
logPath = fso.GetSpecialFolder(2).Path & "\opentext_launch.log"

' 把工作目錄切到本機，避免 CMD 拒絕 UNC
shell.CurrentDirectory = "C:\"

' 用 python（有 console）跑，stderr 導入 log
shell.Run "cmd /c python """ & appScript & """ > """ & logPath & """ 2>&1", 1, False
