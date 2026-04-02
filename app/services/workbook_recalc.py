from __future__ import annotations

import io
import logging
import shutil
import subprocess
import tempfile
from pathlib import Path


log = logging.getLogger(__name__)


def _ps_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def workbook_has_formulas(workbook) -> bool:
    for ws in getattr(workbook, "worksheets", []) or []:
        for row in ws.iter_rows():
            for cell in row:
                value = cell.value
                if cell.data_type == "f" or (isinstance(value, str) and value.lstrip().startswith("=")):
                    return True
    return False


def mark_workbook_for_recalc(workbook) -> None:
    calc = getattr(workbook, "calculation", None)
    if calc is None:
        return
    calc.calcMode = "auto"
    calc.fullCalcOnLoad = True
    calc.forceFullCalc = True
    calc.calcOnSave = True
    calc.calcCompleted = False


def _refresh_with_excel_com(path: Path) -> None:
    script = f"""
$ErrorActionPreference = 'Stop'
$excel = $null
$workbook = $null
try {{
  $excel = New-Object -ComObject Excel.Application
  $excel.Visible = $false
  $excel.DisplayAlerts = $false
  $excel.Calculation = -4105
  $workbook = $excel.Workbooks.Open({_ps_quote(str(path.resolve()))})
  $excel.CalculateFullRebuild()
  $workbook.Save()
}} finally {{
  if ($workbook -ne $null) {{
    $workbook.Close($false)
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($workbook)
  }}
  if ($excel -ne $null) {{
    $excel.Quit()
    [void][System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel)
  }}
  [GC]::Collect()
  [GC]::WaitForPendingFinalizers()
}}
"""
    subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _find_libreoffice_executable() -> str | None:
    return shutil.which("soffice") or shutil.which("libreoffice")


def _refresh_with_libreoffice(path: Path) -> None:
    soffice = _find_libreoffice_executable()
    if not soffice:
        raise FileNotFoundError("LibreOffice not found")
    if path.suffix.lower() != ".xlsx":
        raise ValueError("LibreOffice refresh currently supports .xlsx only")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_root = Path(temp_dir)
        input_path = temp_root / path.name
        output_dir = temp_root / "converted"
        output_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, input_path)

        subprocess.run(
            [
                soffice,
                "--headless",
                "--convert-to",
                "xlsx",
                "--outdir",
                str(output_dir),
                str(input_path),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        converted_path = output_dir / f"{input_path.stem}.xlsx"
        if not converted_path.exists():
            raise FileNotFoundError(f"LibreOffice did not create refreshed workbook: {converted_path}")
        shutil.copy2(converted_path, path)


def refresh_saved_workbook_formula_cache(path: str | Path) -> bool:
    target = Path(path)
    if not target.exists():
        return False

    try:
        _refresh_with_excel_com(target)
        return True
    except Exception as exc:
        log.debug("Excel COM refresh skipped for %s: %s", target, exc)

    try:
        _refresh_with_libreoffice(target)
        return True
    except Exception as exc:
        log.debug("LibreOffice refresh skipped for %s: %s", target, exc)

    return False


def save_workbook_with_recalc(workbook, target_path: str | Path) -> bool:
    mark_workbook_for_recalc(workbook)
    workbook.save(target_path)
    if not workbook_has_formulas(workbook):
        return False
    return refresh_saved_workbook_formula_cache(target_path)


def save_workbook_bytes_with_recalc(workbook, filename: str = "workbook.xlsx") -> io.BytesIO:
    suffix = Path(filename).suffix or ".xlsx"
    safe_name = Path(filename).name or f"workbook{suffix}"
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / safe_name
        save_workbook_with_recalc(workbook, temp_path)
        return io.BytesIO(temp_path.read_bytes())
