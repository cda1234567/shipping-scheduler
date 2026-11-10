"""正航「開 Invoice」（外銷 ST- 單，Flag 501）各資料表的完整欄位清單與預設值。

由 CHIComp03 sys.columns（2026-11-10 唯讀，/tmp/inv_cols.json）依型別產生：
每個欄位 `名稱:型別`，n=數值 0、s=空字串、b=False、z=NULL（image）；順序即正航資料表欄位順序。
寫入時 erp_purchase.build_insert 會與目標資料庫的 sys.columns 逐欄比對，欄位不一致就中止，所以這裡只管預設值。
"""
from __future__ import annotations

from typing import Any, Dict

_TYPE_DEFAULT: Dict[str, Any] = {"n": 0, "s": "", "b": False, "z": None}

_RAW = {
    "TraBillMain": (
        "Flag:n BillNo:s Ref:s IsFormal:b Attn:s Term:n TermContent:s Mark:s SalesID:s Addr:s TEL:s FAX:s "
        "Maker:s Permitter:s UnderValue:n SubDesc:s Agent:s PackingNo:s TradeWay:s CancelBillNo:s Way:s "
        "Shipper:s PerSS:s LoadPort:s DestPort:s Via:s Tariff:s Freight:n BillType:b Payment:s Shipment:s "
        "Insurance:s ValidDate:n ValidDateMemo:s Summary:s CloseDate:n OnBoardDate:n TopTale:s EndTale:s "
        "Memo:s Transed:b TopTale2:s EndTale2:s SignBack:b VoucherNo:s LCAssigned:n CustID:s BillDate:n "
        "CurrID:s ExchRate:n CommRate:n Total:n Commision:n IsCase:b GoodsGivenID:s ToShipping:b UserDef1:s "
        "UserDef2:s MergeOutState:n OriginType:s OriginNo:s MakerID:s PermitterID:s BillStatus:n "
        "IsUpdateKey:b CIQManualNO:s ProjectID:s Address:s LCNo:s IsDirectExport:b CVoucherNo:s HasCheck:b "
        "DeptID:s TranSrvSale:b SumPlus:n CustName:s EngShortName:s Endacasedate:n YearCompressType:n "
        "SumQty:n MasterMark:z TranToFlag:b MergeState:n ParentCompID:s TreeNodeNo:n OriSCNO:s MulOriSCFlag:b "
        "MulTranSCFlag:b MulSingleCondi:n OriCustID:s IsAutoProduct:b IsFinish:b IsExcept:b IsLastOrManual:b "
        "MulOriINVFlag:b MulTranINVFlag:b IsDeduct:b IsBack:b NWUnit:s GWUnit:s VolumeUnit:s Manual:b DueTo:s "
        "CustBillNo:s IsExactBill:b IsContriBute:b InvoicePaperType:n NPOBAN:s CarrierType:s CarrierId1:s "
        "CarrierId2:s ElectronInvoice:n KSType:n BToBOrToC:n IsBToCRegNo:b InvoiceType:n CommonCarrier:b "
        "TaxNo:s InvoAddr:s InvoZipCode:s InvAddress:s InvoStyle:n RelateNumber:s Udef1:s Udef2:s Udef3:s "
        "Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s "
        "Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s "
        "Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n "
        "Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n "
        "Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n "
        "Udef59:n Udef60:n "
    ),
    "TraBillSub": (
        "Flag:n BillNo:s RowNo:n SerNo:n BillDate:n ItemNo:s Description:s Quantity:n Unit:s Price:n Amount:n "
        "SubDesc:s CUFT:n NW:n GW:n QtyCtn:n Mark:s TransedQty:n OnBoardDay:n OriginBillNo:s IsManu:b "
        "ArrangeNo:s QtyRemain:n TranType:n FromNO:s FromRow:n POBillNO:s NotStockASQty:n ToPOQty:n "
        "GoodsGivenNO:s AssemblerQty:n NotStockNASQty:n CancelBillNo:s ItemRemark:s ReportPrice:n HasCheck:b "
        "HasBom:b SUnitID:s SPrice:n SQuantity:n UnitRelation:n IsGift:b BClearTranQty:n EQuantity:n "
        "EUnitID:s EUnitRelation:n TranSrvSale:b StdCost:n UnValidQty:n YearCompressType:n "
        "NotTranPackingQty:n NotTranInvQty:n TraMlAmount:n TCUFT:n TNW:n TGW:n IsOdd:b OriginRowNo:n BoxID:s "
        "HasBox:b HasCharacter:b HasPic:b BoxQty:n MatchNO:s IsTraToLendFlag:b AccBookID:s DeclareQty:n "
        "LegalQty:n LegalQtyB:n DeclareVer:n CIQID:s HasCIQBom:b CloseID:s InvNO:s InvRow:n InvTranType:n "
        "InvFromNo:s InvFromRow:n RelateNumber:s Udef1:s Udef2:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n "
        "Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s "
        "Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n "
        "Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n "
        "Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n "
        "Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "comProdRec": (
        "Flag:n BillNO:s RowNO:n SerNO:n BillDate:n ProdID:s ProdName:s CostAvg:n Quantity:n QuanComb:n "
        "MLPrice:n MLAmount:n AdjustStyle:n WareID:s WareOut:s MLDist:n QtyRemain:n TranType:n FromNO:s "
        "FromRow:n ParentRowNO:n TaxRate:n TaxAmt:n HaveBatch:b NeedUpdate:b CostForAcc:n Weight:n "
        "CostAvgOut:n HasCheck:b Price:n DisCount:n Amount:n ItemRemark:s SUnitID:s SPrice:n SQuantity:n "
        "UnitRelation:n EQuantity:n EUnitID:s EUnitRelation:n MLTaxAmt:n StdCost:n YearCompressType:n IsOdd:b "
        "BoxID:s BoxQty:n HasBox:b HasCharacter:b HasPic:b MatchNO:s ConfigNO:s Detail:s CurVersion:s "
        "MainConfigNo:s HasSerial:b Udef1:s Udef2:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n "
        "Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s "
        "Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n "
        "Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n "
        "Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n "
        "Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "comBillAccounts": (
        "Flag:n SysID:n FundBillNo:s BillDate:n CustFlag:n CustID:s DueTo:s AccMonth:n InvoiceNo:s TaxType:n "
        "CurrID:s ExchRate:n Total:n Tax:n CashPay:n VisaPay:n OffSet:n Discount:n Commision:n ComOffSet:n "
        "ComDiscount:n LocalTotal:n LocalTax:n LocalCashPay:n LocalVisaPay:n LocalOffSet:n LocalComm:n "
        "LocalComOffSet:n PrepayDay:n EncashDay:n DeptID:s ProjectID:s Status:n MergeOutState:n InvoStyle:n "
        "Salesman:s CashPayStyle:s VisaPayStyle:s OtherPayStyle:s OtherPay:n LocOtherPay:n HasCheck:b "
        "NoCheckOffSet:n NoCheckDisCount:n NoChkComOffSet:n NoChkComDisCount:n NoChkLocalOffSet:n "
        "NoChkLocalComOffSet:n CostFlag:n PriceOfTax:b AddrID:s ZipCode:s CustAddress:s ContactPerson:s "
        "LinkManProf:s ContactPhone:s VoucherNO:s CVoucherNO:s MakerID:s Maker:s PermitterID:s Permitter:s "
        "UDef1:s UDef2:s Remark:s InvoFlag:n InvoBillNO:s YearCompressType:n SumDist:n InvoIsCancel:b "
        "ExportState:n ExportDate:n ExportID:s HasApplyToCtoms:b RetailBillNO:s IsRetailBillAutoCreate:b "
        "Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s "
        "Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s "
        "Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n "
        "Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n "
        "Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n "
        "Udef59:n Udef60:n UserDef1:s UserDef2:s FaxNo:s "
    ),
    "comWareAmount": (
        "ProdID:s WareID:s SafeStock:n BQuantity:n BLendQuan:n BBorrowQuan:n BCost:n Cost:n Quantity:n "
        "LendQuan:n BorrowQuan:n Source:b TotalCost:n LatestInDate:n LatestOutDate:n LatestPurchDate:n "
        "LatestSalesDate:n BCQty:n BTotalCost:n BLabour:n BMaterial:n BManufacture:n BMachining:n CLabour:n "
        "CMaterial:n CManufacture:n CMachining:n BCInQty:n BCOutQty:n BCBorrowQty:n BCLendQty:n Remark:s "
        "StorageID:s POSSaleQty:n UpdateKey:n "
    ),
    "StkYearMonthQty": (
        "YearMonth:n ProdID:s WareID:s Quantity:n "
    ),
    "stkYearMonthAvg": (
        "YearMonth:n ProdID:s WareID:s BatchID:s InOutCost:n InOutQuan:n AtLastQuan:n MonthCost:n OnlyCost:b "
        "AtLastCost:n InQuan:n InCost:n Labour:n Material:n Manufacture:n Machining:n clLabour:n clMaterial:n "
        "clManufacture:n clMachining:n sBatchID:s "
    ),
    "comChangeLog": (
        "ProgID:s ChangeTime:n IPAddr:s UserName:s PKValue:s Action:n Datas:z ComputerName:s WinUser:s Flag:n "
        "OldValue:s WinLang:n IsChiDataSet:b ActionName:s "
    ),
}


def _parse(raw: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in raw.split():
        name, _, kind = item.partition(":")
        out[name] = _TYPE_DEFAULT[kind]
    return out


DEFAULTS: Dict[str, Dict[str, Any]] = {table: _parse(raw) for table, raw in _RAW.items()}


def defaults(table: str) -> Dict[str, Any]:
    """該資料表所有欄位的預設值（新字典，可直接覆寫）。"""
    return dict(DEFAULTS[table])
