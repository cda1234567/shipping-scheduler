import pytest


@pytest.fixture(autouse=True)
def _clear_moq_receipt_cache():
    """最近進貨量（MOQ 估算）有程序內快取與失敗退避，每個測試前清掉以免互相影響。"""
    from app.services import moq

    moq.clear_receipt_cache()
    yield
    moq.clear_receipt_cache()
