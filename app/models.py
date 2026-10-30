"""
Pydantic 資料模型 — 用於 API request/response 和資料驗證。
"""
from __future__ import annotations
from enum import Enum
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, validator
import math
import re

from .services.date_infer import normalize_date_text


# ── Enums ─────────────────────────────────────────────────────────────────────

class OrderStatus(str, Enum):
    PENDING    = "pending"
    MERGED     = "merged"
    DISPATCHED = "dispatched"
    COMPLETED  = "completed"
    CANCELLED  = "cancelled"


class ShortageDecision(str, Enum):
    NONE               = "None"
    CREATE_REQUIREMENT = "CreateRequirement"
    MARK_HAS_PO        = "MarkHasPO"
    IGNORE_ONCE        = "IgnoreOnce"
    SHORTAGE           = "Shortage"


class AlertType(str, Enum):
    CUSTOMER_MATERIAL  = "customer_material"
    DELIVERY_CHANGE    = "delivery_change"
    CANCELLATION       = "cancellation"
    SHORTAGE_WARNING   = "shortage_warning"
    BATCH_MERGE_DONE   = "batch_merge_done"


# ── BOM models ────────────────────────────────────────────────────────────────

class BomComponent(BaseModel):
    part_number: str
    description: str = ""
    qty_per_board: float = 0.0
    scrap_factor: float = 0.0
    needed_qty: float = 0.0
    prev_qty_cs: float = 0.0
    is_dash: bool = False
    is_customer_supplied: bool = False
    source_row: Optional[int] = None
    source_sheet: str = ""


class BomFile(BaseModel):
    id: str
    filename: str
    path: str
    po_number: int
    model: str = ""
    pcb: str = ""
    group_model: str = ""
    order_qty: float = 0.0
    components: list[BomComponent] = Field(default_factory=list)
    uploaded_at: str = ""
    source_filename: str = ""
    source_format: str = ""
    is_converted: bool = False


class BomEditorComponentUpdate(BaseModel):
    source_row: int
    part_number: str
    description: str = ""
    qty_per_board: float = 0.0
    scrap_factor: Optional[float] = Field(default=None, ge=0, lt=1)
    needed_qty: float = 0.0
    prev_qty_cs: float = 0.0
    is_dash: bool = False


class BomEditorSaveRequest(BaseModel):
    po_number: int = 0
    model: str = ""
    pcb: str = ""
    group_model: str = ""
    order_qty: float = 0.0
    components: list[BomEditorComponentUpdate] = Field(default_factory=list)


# ── Schedule row（排程表解析用） ──────────────────────────────────────────────

class ScheduleRow(BaseModel):
    po_number: int
    model: str
    pcb: str
    order_qty: float
    balance_qty: Optional[float] = None
    ship_date: Optional[str] = None
    remark: str = ""
    row_index: int = 0
    code: str = ""


# ── Metadata（保留向後相容，遷移期用） ────────────────────────────────────────

class Metadata(BaseModel):
    main_file_path: str = ""
    main_filename: str = ""
    main_loaded_at: str = ""
    main_part_count: int = 0
    schedule_file_path: str = ""
    schedule_filename: str = ""
    schedule_loaded_at: str = ""
    bom_files: dict[str, BomFile] = Field(default_factory=dict)
    bom_order: list[str] = Field(default_factory=list)
    manual_order: list[int] = Field(default_factory=list)
    row_codes: dict[str, str] = Field(default_factory=dict)
    completed_rows: list[int] = Field(default_factory=list)


# ── Request models ────────────────────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    code: str = Field(min_length=1, max_length=100)
    po_number: str = Field(min_length=1, max_length=100)
    model: str = Field(min_length=1, max_length=200)
    pcb: str = Field(min_length=1, max_length=200)
    order_qty: float = Field(gt=0, allow_inf_nan=False)
    balance_qty: float = Field(ge=0, allow_inf_nan=False)
    ship_date: date
    delivery_date: date
    remark: str = Field(default="", max_length=2000)
    insert_before_code: str = Field(default="", max_length=100)

    @validator("code", "po_number", "model", "pcb", "remark", "insert_before_code", pre=True)
    def strip_text(cls, value):
        return value.strip() if isinstance(value, str) else value

    @validator("ship_date", "delivery_date", pre=True)
    def infer_missing_year(cls, value):
        return normalize_date_text(value)  # 「1/29」這種沒有年份的文字依今天推斷年份

    @validator("insert_before_code")
    def validate_insert_code(cls, value):
        if value and not re.fullmatch(r"\d+-\d+", value):
            raise ValueError("插入位置必須是完整單據編號，例如 10-0")
        return value


class ReorderRequest(BaseModel):
    order_ids: list[int]


class UpdateDeliveryRequest(BaseModel):
    delivery_date: str

    @validator("delivery_date", pre=True)
    def infer_missing_year(cls, value):
        return normalize_date_text(value)


class BatchMergeRequest(BaseModel):
    order_ids: list[int]
    reset_stored: bool = False


class BatchDispatchRequest(BaseModel):
    order_ids: list[int]
    decisions: dict[str, str] = Field(default_factory=dict)
    supplements: dict[str, float] = Field(default_factory=dict)
    order_decisions: dict[str, dict[str, str]] = Field(default_factory=dict)
    order_supplements: dict[str, dict[str, float]] = Field(default_factory=dict)
    order_supplement_notes: dict[str, dict[str, str]] = Field(default_factory=dict)
    sample_order_ids: list[int] = Field(default_factory=list)
    reset_stored: bool = False


class DecisionRequest(BaseModel):
    decisions: dict[str, str] = Field(default_factory=dict)
    supplements: dict[str, float] = Field(default_factory=dict)
    is_sample: bool = False


class RowCodeRequest(BaseModel):
    code: str


class UpdateModelRequest(BaseModel):
    model: str


class MoveCompletedFolderRequest(BaseModel):
    folder: str
    new_parent: str = ""


class DatabaseBackupSettingsRequest(BaseModel):
    enabled: bool = True
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    keep_count: int = Field(ge=1, le=365)


class DatabaseBackupRestoreRequest(BaseModel):
    backup_name: str = Field(min_length=1)


class EditAuthLoginRequest(BaseModel):
    password: str = Field(min_length=1, max_length=128)


class StPackageUpdateRequest(BaseModel):
    package_text: str = ""


class InventoryCountStartRequest(BaseModel):
    cutoff_date: str = ""
    cutoff_batch_code: str = ""


class BomSubstitutionRuleRequest(BaseModel):
    model: str
    old_part_number: str
    new_part_number: str
    new_per_old_ratio: float = Field(default=1, gt=0)
    strategy: str = "old_first"
    effective_from_code: str = ""
    note: str = ""


class OrderSubstitutionAllocationRequest(BaseModel):
    old_qty: float = Field(ge=0)
    new_qty: float = Field(ge=0)


# ── Helpers ───────────────────────────────────────────────────────────────────

def calc_suggested_qty(shortage: float, moq: float) -> float:
    if moq and moq > 0:
        return math.ceil(shortage / moq) * moq
    return shortage


class UpdateMoqRequest(BaseModel):
    part_number: str
    moq: float = Field(ge=0)


# ── Defective Records ────────────────────────────────────────────────────────

class DefectiveAction(str, Enum):
    REWORK          = "rework"
    SCRAP           = "scrap"
    RETURN_SUPPLIER = "return_supplier"
    REPLACE         = "replace"
    OTHER           = "other"


class CreateDefectiveRequest(BaseModel):
    order_id: Optional[int] = None
    part_number: str
    description: str = ""
    defective_qty: float = Field(gt=0)
    action_taken: str = ""
    action_note: str = ""
    reported_by: str = ""


class UpdateDefectiveRequest(BaseModel):
    part_number: Optional[str] = None
    description: Optional[str] = None
    defective_qty: Optional[float] = None
    action_taken: Optional[str] = None
    action_note: Optional[str] = None
    reported_by: Optional[str] = None


class SupplementPartRequest(BaseModel):
    part_number: str
    supplement_qty: float
    note: str = ""


class OverrunDeductionRequest(BaseModel):
    model: str
    extra_pcs: float = Field(gt=0)
    reason: str = ""
    note: str = ""
    reported_by: str = ""


class OverrunImportConfirmItem(BaseModel):
    source_row: int = Field(ge=1)
    part_number: str
    defective_qty: float = Field(gt=0)
    description: str = ""
    action: str = "deduct"
    target_part_number: str = ""


class OverrunImportConfirmRequest(BaseModel):
    source_filename: str = ""
    title: str = ""
    mo_info: str = ""
    items: list[OverrunImportConfirmItem] = Field(default_factory=list)


class DefectiveImportConfirmItem(BaseModel):
    source_row: int = Field(ge=1)
    part_number: str
    defective_qty: float = Field(gt=0)
    description: str = ""
    action: str = "deduct"
    target_part_number: str = ""


class DefectiveImportConfirmRequest(BaseModel):
    batch_id: Optional[int] = None
    source_filename: str = ""
    items: list[DefectiveImportConfirmItem] = Field(default_factory=list)


class DefectiveReplayRequest(BaseModel):
    cutoff: str


# ── 常用料號低庫存提醒 ────────────────────────────────────────────────────────

class LowStockAddRequest(BaseModel):
    part_number: str
    threshold_qty: Optional[float] = None
    lead_time_days: Optional[float] = None
    note: str = ""


class LowStockUpdateRequest(BaseModel):
    # 只有請求中實際出現的欄位才會更新（用 .dict(exclude_unset=True)）；threshold_qty / lead_time_days 為 null = 恢復自動
    part_number: str
    threshold_qty: Optional[float] = None
    lead_time_days: Optional[float] = None
    enabled: Optional[bool] = None
    note: Optional[str] = None


class LowStockRemoveRequest(BaseModel):
    part_number: str


class LowStockSettingsRequest(BaseModel):
    lookback_days: Optional[float] = None
    suggest_lookback_days: Optional[float] = None
    min_bills: Optional[float] = None
    safety_factor: Optional[float] = None
    min_cover_days: Optional[float] = None
    default_lead_days: Optional[float] = None
    lead_lookback_days: Optional[float] = None
    lead_sample_size: Optional[float] = None
    actual_lead_ratio: Optional[float] = None
    actual_lead_min_gap_days: Optional[float] = None
    on_order_max_overdue_days: Optional[float] = None


# ── 正航 ERP 採購單 ───────────────────────────────────────────────────────────

class ErpPoAllocation(BaseModel):
    order_id: int = 0
    order_code: str = ""
    model: str = ""
    customer_po: str = ""
    ship_date: str = ""
    qty: float
    kind: str = ""  # order | extra | manual（空值依 order_id 判斷）


class ErpPoLine(BaseModel):
    part_number: str
    qty: float
    price: float
    pre_in_date: str  # YYYY-MM-DD
    allocations: Optional[list[ErpPoAllocation]] = None


class ErpPoRequest(BaseModel):
    supplier_id: str
    remark: str = ""
    salesman: Optional[str] = None
    lines: list[ErpPoLine]


class ErpPoCreateRequest(ErpPoRequest):
    confirm: bool = False
    allow_duplicate: bool = False  # 明細本站已開過時，使用者確認後才帶 true
    confirm_missing_postprocess: bool = False  # 鈞洋件有後段加工對應但加工件不在同一批時，使用者確認後才帶 true


# ── 一次收料（收料→驗收→進貨）──────────────────────────────────────────────────

class ErpReceiveLine(BaseModel):
    po_no: str
    row_no: int
    qty: float
    price: Optional[float] = None  # 空 = 沿用採購單單價


class ErpReceiveRequest(BaseModel):
    target: Optional[str] = None  # 空 = ERP_RECEIVE_TARGET（沒設定時 ERP_WRITE_TARGET）
    rm_date: Optional[int] = None  # 進貨（發票）日 yyyymmdd，空 = 今天
    lines: list[ErpReceiveLine]
    confirm: bool = False  # 超收時使用者確認後才帶 true
    client_token: str = ""  # 前端每次開啟預覽產生，防重複送出
    update_cost: Optional[bool] = None  # 是否更新平均成本；空 = 環境變數 ERP_RECEIVE_UPDATE_COST（預設 1）


# ── 缺料採購 ──────────────────────────────────────────────────────────────────

class ShortagePoCategoryRequest(BaseModel):
    part_number: str
    category: str  # smt | backend


class ShortagePoRequest(BaseModel):
    groups: list[ErpPoRequest]
    order_ids: list[int] = []  # 產生缺料清單時勾選的訂單，用來重算各料的訂單分配


class ShortagePoCreateRequest(ShortagePoRequest):
    confirm: bool = False
    allow_duplicate: bool = False
    confirm_missing_postprocess: bool = False


# ── 轉換料 A→B ────────────────────────────────────────────────────────────────

class PartConversionRequest(BaseModel):
    part_a: str
    part_b: str
    dry_run: bool = False


class PartConversionRestoreRequest(BaseModel):
    force: bool = False  # 轉換後 BOM 又被改過時，強制還原（會覆蓋那些修改）


# ── 客人改交期 → 廠商提前清單 ─────────────────────────────────────────────────

class PullinConfirmRequest(BaseModel):
    order_ids: list[int]
    new_ship_date: str
    update_order_ship_date: bool = False  # 預設不動訂單出貨日
    dry_run: bool = False

    @validator("new_ship_date", pre=True)
    def infer_missing_year(cls, value):
        return normalize_date_text(value)


class PullinLine(BaseModel):
    line_key: str = ""
    supplier_id: str = ""
    supplier_name: str = ""
    bill_no: str = ""
    row_no: int = 0
    part_number: str = ""
    qty: float = 0
    po_date: str = ""
    requested_date: str = ""
    days_earlier: int = 0
    order_ids: list[int] = []


class PullinStatusRequest(BaseModel):
    lines: list[PullinLine]
    status: str  # 未詢問 | 已詢問 | 可提前 | 不能提前
    note: Optional[str] = None


# ── 鈞洋 後段加工對應 ─────────────────────────────────────────────────────────

class PostprocessLinkCreate(BaseModel):
    base_part: str
    process_part: str
    ratio: float = 1
    note: str = ""


class PostprocessLinkUpdate(BaseModel):
    process_part: Optional[str] = None
    ratio: Optional[float] = None
    enabled: Optional[bool] = None
    note: Optional[str] = None


class PostprocessSuggestRequest(BaseModel):
    use_erp: bool = True
