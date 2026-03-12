Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
command = "pyw -3 """ & scriptDir & "\desktop_app.py"""

shell.CurrentDirectory = scriptDir
shell.Run command, 0, False
