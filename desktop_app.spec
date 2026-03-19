# -*- mode: python ; coding: utf-8 -*-

from pathlib import Path

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

project_root = Path(globals().get("SPECPATH", Path.cwd())).resolve()
datas = [
    (str(project_root / "static"), "static"),
    (str(project_root / "templates"), "templates"),
    (str(project_root / "config.yaml"), "."),
    (str(project_root / "desktop_client.json.example"), "."),
]
datas += collect_data_files("webview")

hiddenimports = sorted(set(
    collect_submodules("app")
    + collect_submodules("uvicorn")
    + collect_submodules("webview")
))

a = Analysis(
    ["desktop_app.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="OpenTextDesktop",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon=str(project_root / "static" / "assets" / "opentext_app_icon.ico"),
)

coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="OpenTextDesktop",
)
