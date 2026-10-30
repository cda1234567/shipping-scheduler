"""正航「收料 → 驗收 → 進貨」各資料表的完整欄位清單與預設值（自動由 Andy 手動鏈 CHIComp13 20261030001 的 SELECT * 產生）。

格式：每個欄位 `名稱:型別`，型別 n=數值 0、s=空字串、b=False、z=NULL；順序即正航資料表欄位順序。
實際寫入時 erp_purchase.build_insert 會與目標資料庫的 sys.columns 逐欄比對，欄位不一致就中止，所以這裡只管「沒特別指定的欄位」的預設值。
"""
from __future__ import annotations

from typing import Any, Dict

_TYPE_DEFAULT: Dict[str, Any] = {"n": 0, "s": "", "b": False, "z": None}

_RAW = {
    "ordExamineMain": (
        "Flag:n ExamineNO:n ExamineDate:n CustomerID:s Examiner:s Maker:s Permitter:s Remark:s "
        "MergeOutState:n MakerID:n PermitterID:s HasCheck:b DepartID:s YearCompressType:n DeliverBillNO:s "
        "Udef1:s Udef2:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s "
        "Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s "
        "Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n "
        "Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n "
        "Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n "
        "Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "ordExamineSub": (
        "Flag:n ExamineNO:n RowNO:n SerNO:n ExamineDate:n ProdID:s ProdName:s TranType:n FromNO:n FromRow:n "
        "Quantity:n QtyRemain:n Unit:s ExmineType:s Description:s ItemRemark:s MergeOutState:n HasCheck:b "
        "sQuantity:n UnitRelation:n sUnitID:s srcFrom:n Detail:s EQuantity:n EUnitID:s EUnitRelation:n "
        "YearCompressType:n TQMCheckBillNo:s TQMChkReqNo:s IsOdd:b BoxID:s HasBox:b HasCharacter:b HasPic:b "
        "BoxQty:n MatchNO:s Udef1:s Udef2:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n "
        "Udef11:n Udef12:s Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s "
        "Udef22:s Udef23:s Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n "
        "Udef33:n Udef34:n Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n "
        "Udef44:n Udef45:n Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n "
        "Udef55:n Udef56:n Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "stkBillMain": (
        "Flag:n BillNO:n AddrID:s ZipCode:s CustAddress:s ContactPerson:s ContactPhone:s PriceofTax:b "
        "SalesMan:s Maker:s VoucherNO:s Permitter:s SumAmtATax:n SumNum:n TaxType:n GatherStyle:n "
        "GatherDelay:n CheckStyle:n CheckDelay:n UDef1:s UDef2:s InvoiceN0T1:s MakerID:s PermitterID:s "
        "DataVer:n LinkManProf:s IsDeduct:b SignBack:b CVoucherNO:s Detail:s IsSaleCheck:b TranSrvSale:b "
        "GatherOther:s CheckOther:s SumTransferFee:n SumOtherFee:n SumFee:n BillStatus:n ShareType:n Payed:n "
        "PayedLocal:n NotPayed:n NotPayedLocal:n RealPay:n RealPayLocal:n InCost:n ExchProfit:n SumCost:n "
        "DeliverBillNo:s TranToAdjustNo:s CustBillNo:s IsExactBill:b IsContriBute:b InvoicePaperType:n "
        "NPOBAN:s CarrierType:s CarrierId1:s CarrierId2:s ElectronInvoice:n KSType:n BToBOrToC:n "
        "IsBToCRegNo:b InvoiceType:n InvoAddr:s InvoZipCode:s InvAddress:s TaxNo:s CommonCarrier:b "
        "RelateNumber:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s "
        "Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s "
        "Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n "
        "Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n "
        "Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n "
        "Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "stkBillSub": (
        "Flag:n BillNO:n RowNO:n SerNO:n ConvertRate:n Unit:s Price:n Discount:n Amount:n ItemRemark:s "
        "HaveBatch:b IsGift:b CustBillNo:s Mark:s Detail:s TranSrvSale:b TransferFee:n OtherFee:n TotalFee:n "
        "StkMlAmount:n LocalPayment:n IsTailPurchase:b PriceType:n RelateNumber:s "
    ),
    "comBillAccounts": (
        "Flag:n SysID:n FundBillNo:n BillDate:n CustFlag:n CustID:s DueTo:s AccMonth:n InvoiceNo:s TaxType:n "
        "CurrID:s ExchRate:n Total:n Tax:n CashPay:n VisaPay:n OffSet:n Discount:n Commision:n ComOffSet:n "
        "ComDiscount:n LocalTotal:n LocalTax:n LocalCashPay:n LocalVisaPay:n LocalOffSet:n LocalComm:n "
        "LocalComOffSet:n PrepayDay:n EncashDay:n DeptID:s ProjectID:s Status:n MergeOutState:n InvoStyle:n "
        "Salesman:s CashPayStyle:n VisaPayStyle:n OtherPayStyle:s OtherPay:n LocOtherPay:n HasCheck:b "
        "NoCheckOffSet:n NoCheckDisCount:n NoChkComOffSet:n NoChkComDisCount:n NoChkLocalOffSet:n "
        "NoChkLocalComOffSet:n CostFlag:n PriceOfTax:b AddrID:n ZipCode:n CustAddress:s ContactPerson:s "
        "LinkManProf:s ContactPhone:s VoucherNO:s CVoucherNO:s MakerID:n Maker:s PermitterID:s Permitter:s "
        "UDef1:s UDef2:s Remark:s InvoFlag:n InvoBillNO:s YearCompressType:n SumDist:n InvoIsCancel:b "
        "ExportState:n ExportDate:n ExportID:s HasApplyToCtoms:b RetailBillNO:s IsRetailBillAutoCreate:b "
        "Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s "
        "Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s "
        "Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n "
        "Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n "
        "Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n "
        "Udef59:n Udef60:n UserDef1:s UserDef2:s FaxNo:s "
    ),
    "comProdRec": (
        "Flag:n BillNO:n RowNO:n SerNO:n BillDate:n ProdID:s ProdName:s CostAvg:n Quantity:n QuanComb:n "
        "MLPrice:n MLAmount:n AdjustStyle:n WareID:n WareOut:s MLDist:n QtyRemain:n TranType:n FromNO:n "
        "FromRow:n ParentRowNO:n TaxRate:n TaxAmt:n HaveBatch:b NeedUpdate:b CostForAcc:n Weight:n "
        "CostAvgOut:n HasCheck:b Price:n DisCount:n Amount:n ItemRemark:s SUnitID:s SPrice:n SQuantity:n "
        "UnitRelation:n EQuantity:n EUnitID:s EUnitRelation:n StdCost:n MLTaxAmt:n YearCompressType:n IsOdd:b "
        "BoxID:s BoxQty:n HasBox:b HasCharacter:b HasPic:b MatchNO:s ConfigNO:s Detail:s CurVersion:s "
        "MainConfigNo:s HasSerial:b Udef1:s Udef2:s Udef3:s Udef4:s Udef5:s Udef6:n Udef7:n Udef8:n Udef9:n "
        "Udef10:n Udef11:n Udef12:s Udef13:s Udef14:s Udef15:s Udef16:s Udef17:s Udef18:s Udef19:s Udef20:s "
        "Udef21:s Udef22:s Udef23:s Udef24:s Udef25:s Udef26:s Udef27:n Udef28:n Udef29:n Udef30:n Udef31:n "
        "Udef32:n Udef33:n Udef34:n Udef35:n Udef36:n Udef37:n Udef38:n Udef39:n Udef40:n Udef41:n Udef42:n "
        "Udef43:n Udef44:n Udef45:n Udef46:n Udef47:n Udef48:n Udef49:n Udef50:n Udef51:n Udef52:n Udef53:n "
        "Udef54:n Udef55:n Udef56:n Udef57:n Udef58:n Udef59:n Udef60:n "
    ),
    "comWareAmount": (
        "ProdID:s WareID:n SafeStock:n BQuantity:n BLendQuan:n BBorrowQuan:n BCost:n Cost:n Quantity:n "
        "LendQuan:n BorrowQuan:n Source:b TotalCost:n LatestInDate:n LatestOutDate:n LatestPurchDate:n "
        "LatestSalesDate:n BCQty:n BTotalCost:n BLabour:n BMaterial:n BManufacture:n BMachining:n CLabour:n "
        "CMaterial:n CManufacture:n CMachining:n BCInQty:n BCOutQty:n BCBorrowQty:n BCLendQty:n Remark:s "
        "StorageID:s POSSaleQty:n UpdateKey:n "
    ),
    "StkYearMonthQty": (
        "YearMonth:n ProdID:s WareID:n Quantity:n "
    ),
    "stkYearMonthAvg": (
        "YearMonth:n ProdID:s WareID:n BatchID:s InOutCost:n InOutQuan:n AtLastQuan:n MonthCost:n OnlyCost:b "
        "AtLastCost:n InQuan:n InCost:n Labour:n Material:n Manufacture:n Machining:n clLabour:n clMaterial:n "
        "clManufacture:n clMachining:n sBatchID:s "
    ),
    "comCustAccounts": (
        "Flag:n SysID:n ID:s CurrID:s StartExchRate:n StartAdvExchRate:n StartCommExchRate:n StartAdvRecv:n "
        "LocStartAdvRecv:n StartAdvOffseted:n LocStartAdvOffseted:n NoChkStartAdvOffSet:n "
        "NoChkLocStartAdvOffSet:n CurAdvRecv:n LocCurAdvRecv:n NoChkCurrAdv:n NoChkLocCurrAdv:n "
        "NoChkCurrAdvOffSet:n StartReceivable:n LocStartReceivable:n StartOffseted:n LocStartOffseted:n "
        "NoChkStartOffSet:n NoChkLocStartOffSet:n CurReceivable:n LocCurReceivable:n NoChkCurrRece:n "
        "NoChkLocCurrRece:n NoChkCurrReceOffSet:n StartCommission:n LocStartCommission:n StartComOffseted:n "
        "LocStartComOffseted:n NoChkStartComOffSet:n NoChkLocStartComOffSet:n CurCommission:n "
        "LocCurCommission:n NoChkCurrCom:n NoChkLocCurrCom:n NoChkCurrComOffSet:n ShiftAddMoney:n "
        "LocShiftAddMoney:n ShiftUseMoney:n LocShiftUseMoney:n NoChkShiftUseMoney:n NoChkLocShiftUseMoney:n "
        "ShiftStartRece:n ShiftStartAdv:n LocShiftStartAdv:n NoChkLocCurrReceOffSet:n NoChkLocCurrAdvOffSet:n "
        "NoChkLocCurrComOffset:n "
    ),
    "comChangeLog": (
        "ProgID:s ChangeTime:n IPAddr:s UserName:n PKValue:n Action:n Datas:z ComputerName:s WinUser:s Flag:n "
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

