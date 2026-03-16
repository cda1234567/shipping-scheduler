"""
Pydantic 資料模型 — 用於 API request/response 和資料驗證。
"""
from __future__ import annotations
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
import math


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

class ReorderRequest(BaseModel):
    order_ids: list[int]


class UpdateDeliveryRequest(BaseModel):
    delivery_date: str


class BatchMergeRequest(BaseModel):
    order_ids: list[int]


class BatchDispatchRequest(BaseModel):
    order_ids: list[int]
    decisions: dict[str, str] = Field(default_factory=dict)
    supplements: dict[str, float] = Field(default_factory=dict)


class DecisionRequest(BaseModel):
    decisions: dict[str, str] = Field(default_factory=dict)
    supplements: dict[str, float] = Field(default_factory=dict)


class RowCodeRequest(BaseModel):
    code: str


class UpdateModelRequest(BaseModel):
    model: str


class DatabaseBackupSettingsRequest(BaseModel):
    enabled: bool = True
    hour: int = Field(ge=0, le=23)
    minute: int = Field(ge=0, le=59)
    keep_count: int = Field(ge=1, le=365)


class DatabaseBackupRestoreRequest(BaseModel):
    backup_name: str = Field(min_length=1)


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
