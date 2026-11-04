import json
from decimal import Decimal
from pathlib import Path

import pytest

from app.services import rm_print, rm_print_data, rm_print_dispatch

FIXTURE = Path(__file__).parent / "fixtures" / "rm_20261103001.json"


@pytest.fixture()
def fonts():
    pytest.importorskip("reportlab")
    try:
        return rm_print.resolve_fonts()
    except RuntimeError as exc:
        pytest.skip(str(exc))


def sample():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def short_line(i):
    return {"row_no": i, "prod_id": f"P{i:04d}", "prod_name": "電阻", "ware": "ST", "qty": 1, "unit": "PCS",
            "price": 1, "amount": 1, "tax": 0, "remark": ""}


def bill(n):
    d = sample()
    d["lines"] = [short_line(i) for i in range(1, n + 1)]
    d["sum_amount"], d["sum_tax"] = n, 0
    return d


def test_fmt_num():
    assert rm_print.fmt_num(6000, 4) == "6,000.0000"
    assert rm_print.fmt_num(2.2, 3) == "2.200"
    assert rm_print.fmt_num(13860, 0) == "13,860"
    assert rm_print.fmt_num(0.0005, 3) == "0.001"  # 四捨五入（不是銀行家捨入）
    assert rm_print.fmt_num(-1234.5, 0) == "-1,235"
    assert rm_print.fmt_num(None, 0) == "0"


def test_roc_date():
    assert rm_print.roc_date(20261104) == "115/11/04"
    assert rm_print.roc_date("2026-12-30") == "115/12/30"
    assert rm_print.roc_date(None) == ""
    assert rm_print.roc_date(0) == ""


def test_pay_terms():
    assert rm_print.pay_terms_text(1, 30) == "次月 30 天"
    assert rm_print.pay_terms_text(2, 30) == ""  # 其他種類不猜文字
    assert rm_print.pay_terms_text(2, 30, "貨到付現") == "貨到付現"  # 有正航文字就照印
    assert rm_print.pay_terms_text(0, 0) == ""


def test_tax_type_text():
    assert rm_print.tax_type_text(0) == "應稅"
    assert rm_print.tax_type_text(None) == "應稅"
    for t in (1, 2, 3, 4):
        assert rm_print.tax_type_text(t) == "免稅"


def test_totals_and_unpaid():
    t = rm_print.totals(sample())
    assert t["sum_amount"] == Decimal(13200) and t["sum_tax"] == Decimal(660) and t["total"] == Decimal(13860)
    assert t["unpaid"] == Decimal(13860)
    d = sample()
    d.update(discount=100, cash_pay=760)
    assert rm_print.totals(d)["unpaid"] == Decimal(13000)
    d = sample()
    d.pop("sum_amount"), d.pop("sum_tax")
    assert rm_print.totals(d)["total"] == Decimal(13860)  # 沒有表頭合計時用明細加總


def test_sample_fits_half_page(fonts):
    lay = rm_print.layout(sample())
    assert lay["page_count"] == 1
    assert lay["fits_half_page"] is True
    assert abs(lay["bottom_mm"] - 133.42) < 0.05  # 與正航樣張的簽核列位置一致
    assert lay["pages"][0]["rows"][0]["lines"] == 4  # 品名規格折成 4 行（同樣張）


def test_half_page_threshold(fonts):
    n = rm_print.max_single_line_rows_in_half_page()
    assert n == 5
    assert rm_print.layout(bill(n))["fits_half_page"] is True
    assert rm_print.layout(bill(n + 1))["fits_half_page"] is False


def test_half_page_env(monkeypatch, fonts):
    monkeypatch.setenv("RM_PRINT_HALF_PAGE_MM", "100")
    assert rm_print.layout(sample())["fits_half_page"] is False


def test_printer_choice_and_env(monkeypatch):
    monkeypatch.delenv("RM_PRINT_PRINTER_HALF", raising=False)
    monkeypatch.delenv("RM_PRINT_PRINTER_FULL", raising=False)
    assert rm_print.choose_printer(True) == "NPI975BEF (HP LaserJet M111w)"
    assert rm_print.choose_printer(False) == "HP Color LaserJet M652 [7D2006]"
    monkeypatch.setenv("RM_PRINT_PRINTER_HALF", "HALF-X")
    monkeypatch.setenv("RM_PRINT_PRINTER_FULL", "FULL-Y")
    assert rm_print.choose_printer(True) == "HALF-X" and rm_print.choose_printer(False) == "FULL-Y"


def test_multi_page(fonts):
    lay = rm_print.layout(bill(60))
    assert lay["page_count"] >= 2 and lay["fits_half_page"] is False
    assert sum(len(p["rows"]) for p in lay["pages"]) == 60
    assert all(p["footer_top"] is None for p in lay["pages"][:-1])
    assert lay["pages"][-1]["footer_top"] is not None
    for p in lay["pages"]:
        for r in p["rows"]:
            assert r["top"] + r["height"] <= rm_print.PAGE_BOTTOM_MM + 1e-6
    assert lay["bottom_mm"] <= rm_print.PAGE_BOTTOM_MM


def test_render_pdf_pages_and_page_numbers(fonts):
    pdf, info = rm_print.render_pdf(bill(60))
    assert pdf.startswith(b"%PDF")
    assert info["page_count"] >= 2 and info["printer"] == rm_print.choose_printer(False)
    pdf1, info1 = rm_print.render_pdf(sample())
    assert info1["page_count"] == 1 and info1["fits_half_page"] is True
    import re
    assert len(re.findall(rb"/Type /Page\b", pdf)) == info["page_count"]
    assert len(re.findall(rb"/Type /Page\b", pdf1)) == 1
    assert rm_print.page_label(1, 1) == "1" and rm_print.page_label(2, 3) == "2/3"


def test_wrap_long_name(fonts):
    lines = [l.rstrip() for l in rm_print.wrap_text("CRYSTAL,25.000MHZ (+/-50ppm OR BETTER) FUNDAMENTAL,HC-49/S", 36.40)]
    assert lines == ["CRYSTAL,25.000MHZ", "(+/-50ppm OR BETTER)", "FUNDAMENTAL,HC-49/", "S"]


def test_build_from_erp_maps_fields():
    acct = {"FundBillNo": "RM-1", "BillDate": 20261103, "CustID": "P0281", "CurrID": "NTD", "Total": 100, "Tax": 5,
            "TaxType": 0, "PrepayDay": 20261230, "EncashDay": 20261230, "DeptID": "D1", "Salesman": "S1",
            "PriceOfTax": 0, "CustAddress": "地址", "ContactPhone": "02-1", "FaxNo": "02-2", "Maker": "Andy", "InvoiceNo": ""}
    main = {"GatherStyle": 1, "GatherDelay": 30, "InvAddress": ""}
    recs = [{"RowNO": 1, "ProdID": "A", "ProdName": "品名", "Quantity": 2, "Price": 50, "Amount": 100, "TaxAmt": 5, "WareID": "ST"}]
    vendor = {"FullName": "廠商", "TaxNo": "123", "TelePhone1": "02-9"}
    local = {"rm_no": "RM-1", "rm_date": 20261103, "print_date": 20261104, "vendor_id": "P0281", "vendor_name": "x",
             "lines": [{"prod_id": "A", "unit": "PCS"}]}
    d = rm_print_data.build_from_erp(acct, main, recs, vendor, {"PersonName": "李"}, {"DepartName": "工程部"},
                                     {"A": ""}, {}, local)
    assert d["vendor_name"] == "廠商" and d["tax_no"] == "123" and d["vendor_phone"] == "02-9"
    assert d["buyer"] == "李" and d["dept"] == "工程部" and d["pay_terms"] == "次月 30 天"
    assert d["invoice_address"] == "地址"  # 無發票地址時同廠商地址
    assert d["lines"][0]["unit"] == "PCS" and d["lines"][0]["ware"] == "ST"


def test_from_local():
    rec = {"rm_no": "RM-9", "rm_date": 20261104, "vendor_id": "V", "vendor_name": "N", "currency": "NTD",
           "sum_amount": 10, "sum_tax": 0.5,
           "lines_json": json.dumps([{"row_no": 1, "prod_id": "A", "prod_name": "B", "unit": "PCS", "qty": 1,
                                      "price": 10, "amount": 10, "tax": 0.5}])}
    d = rm_print_data.from_local(rec)
    assert d["rm_no"] == "RM-9" and d["lines"][0]["prod_id"] == "A"


def test_helper_disabled_by_default(monkeypatch):
    monkeypatch.delenv("RM_PRINT_HELPER_ENABLED", raising=False)
    assert rm_print_dispatch.helper_config()["enabled"] is False
    with pytest.raises(rm_print_dispatch.PrintHelperError):
        rm_print_dispatch.send_pdf(b"%PDF", "x", "RM")
    monkeypatch.setenv("RM_PRINT_HELPER_ENABLED", "1")
    monkeypatch.delenv("RM_PRINT_HELPER_TOKEN", raising=False)
    assert rm_print_dispatch.helper_config()["enabled"] is False  # 沒 token 不算啟用


def test_print_endpoints(monkeypatch, fonts):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.routers.erp_receive import router
    app = FastAPI()
    app.include_router(router, prefix="/api")
    from app.services.erp_stock import ErpUnavailable

    rec = {"id": 7, "status": "ok", "rm_no": "RM-20261103001", "rm_date": 20261103, "vendor_id": "P0281",
           "vendor_name": "雅士博", "currency": "NTD", "sum_amount": 13200, "sum_tax": 660, "target_db": "CHIComp03",
           "lines_json": json.dumps(sample()["lines"], ensure_ascii=False)}
    from app import database as db
    monkeypatch.setattr(db, "get_erp_receipt", lambda i: rec if int(i) == 7 else None)

    def no_erp(r):
        raise ErpUnavailable("offline")
    monkeypatch.setattr(rm_print_data, "load_from_erp", no_erp)
    monkeypatch.delenv("RM_PRINT_HELPER_ENABLED", raising=False)
    c = TestClient(app)
    r = c.get("/api/erp-receive/receipts/7/print-info")
    assert r.status_code == 200
    j = r.json()
    assert j["fits_half_page"] is True and j["printer"] == rm_print.choose_printer(True)
    assert j["helper_enabled"] is False and j["source"] == "local" and j["warning"]
    r = c.get("/api/erp-receive/receipts/7/print.pdf")
    assert r.status_code == 200 and r.content.startswith(b"%PDF")
    assert r.headers["content-type"] == "application/pdf"
    assert c.get("/api/erp-receive/receipts/8/print-info").status_code == 404
