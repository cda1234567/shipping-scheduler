"""001／002→015 調撥；每批獨立交易，正式公司由調撥專用開關控制。"""
from __future__ import annotations

import hashlib
import json
import os
import socket
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from .. import database as db
from . import erp_purchase as ep, erp_receive as er, st_pending_transfer as pending, po_duplicates, low_stock
from .erp_stock import ErpUnavailable
from .local_time import local_now
from .erp_transfer_schema import DEFAULTS


def resolve_target() -> tuple[str, dict[str, Any]]:
    target = ep._env('ERP_TRANSFER_TARGET', 'CHIComp13')
    if not ep.TARGET_RE.fullmatch(target):
        raise ep.PoValidationError('調撥目標公司格式不正確')
    if target == 'CHIComp03':
        if ep._env('ERP_TRANSFER_TARGET') != target or os.environ.get('ERP_ALLOW_PROD_TRANSFER_CHICOMP03', '0') != '1':
            raise ep.PoValidationError('公司3（正式）尚未開放一鍵調撥，需設定調撥專用目標與開關')
    elif not ep.is_test(target):
        raise ep.PoValidationError('調撥只允許測試公司或明確開放的公司3')
    creds = ep.write_credentials(target)
    if not creds:
        raise ep.PoValidationError('尚未設定調撥目標公司的專用寫入帳號')
    return target, creds


def get_config() -> dict[str, Any]:
    target = ep._env('ERP_TRANSFER_TARGET', 'CHIComp13')
    result = dict(target=target, label=ep.target_label(target), is_production=target == 'CHIComp03', available=False, reason='')
    try:
        resolve_target()
        result['available'] = True
    except Exception as exc:
        result['reason'] = ep._safe_message(exc)
    return result


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def next_number(cur: Any, today: int) -> str:
    cur.execute('SELECT MoveNO FROM stkMoveMain WITH (UPDLOCK, HOLDLOCK) WHERE Flag=400 '
                'AND (MoveNO LIKE %s OR MoveNO LIKE %s)', (f'RM-{today}%', f'{today}%'))
    serial = 0
    for row in cur.fetchall():
        value = str(row[0]).removeprefix('RM-')
        if len(value) == 11 and value[:8] == str(today) and value[8:].isdigit():
            serial = max(serial, int(value[8:]))
    if serial >= 999:
        raise ep.PoValidationError('今日調撥單號已用完')
    return f'RM-{today}{serial+1:03d}'


def one(cur: Any, sql: str, args: tuple) -> dict[str, Any] | None:
    cur.execute(sql, args)
    return ep._one(cur)


def cost_values(product: dict, outgoing: dict, incoming: dict, qty: float, params: dict) -> dict[str, float]:
    dec = lambda x: Decimal(str(x or 0))
    avg_out = dec(ep._get(outgoing, 'Cost') if params['cost_by_ware'] else ep._get(product, 'CAvgCost'))
    price = er._round_cost(avg_out, params)
    amount = float((dec(qty)*dec(price)).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
    avg = float(avg_out)
    if params['cost_by_ware']:
        # Comp13 test behaviour, unverified against 正航.
        new_qty = dec(ep._get(incoming, 'Quantity')) + dec(qty)
        total = dec(ep._get(incoming, 'TotalCost')) + dec(amount)
        avg = er._round_cost(total/new_qty, params) if new_qty > 0 else float(ep._get(incoming, 'Cost') or 0)
    return dict(cost=price, amount=amount, avg=avg, avg_out=float(avg_out))


def build_rows(move_no: str, today: int, rows: list[dict]) -> dict[str, list[dict]]:
    """三份公司3樣本 EUnitRelation 都為0；新單 DataVer 依規格為1（手開樣本為2）。"""
    def row(table: str, **values: Any) -> dict[str, Any]:
        return {**DEFAULTS[table], **values}
    main = row('stkMoveMain', Flag=400, MoveNO=move_no, MoveDate=today,
               Maker=ep._env('ERP_PO_MAKER', ep.DEFAULT_MAKER), MakerID=ep._env('ERP_PO_MAKER_ID', ep.DEFAULT_MAKER_ID),
               SumQty=sum(r['transfer'] for r in rows), DataVer=1, HasCheck=True)
    subs, recs = [], []
    for i, r in enumerate(rows, 1):
        subs.append(row('stkMoveSub', Flag=400, MoveNO=move_no, RowNo=i, SerNo=i, MoveDate=today, Cost=r['cost']))
        recs.append(row('comProdRec', Flag=400, BillNO=move_no, RowNO=i, SerNO=i, BillDate=today,
                        ProdID=r['part'], ProdName=r['name'], WareOut=r.get('ware_out', '001'), WareID='015',
                        Quantity=r['transfer'], EQuantity=r['transfer'], CostAvg=r['avg'], CostAvgOut=r['avg_out'],
                        MLPrice=r['cost'], MLAmount=r['amount'], QtyRemain=1, SQuantity=1, UnitRelation=1,
                        EUnitRelation=0, NeedUpdate=True, HasCheck=True, IsOdd=True))
    return {'stkMoveMain': [main], 'stkMoveSub': subs, 'comProdRec': recs}


_NAME_CACHE: dict[str, dict[str, str]] = {}


def source_names(target: str, batches: list[dict]) -> dict[str, str]:
    """測試公司缺的料號品名，從來源公司（唯讀）一次批次讀取。"""
    key = digest(sorted({r['part'] for b in batches for r in b['rows']}))
    if key not in _NAME_CACHE:
        parts = sorted({r['part'] for b in batches for r in b['rows']})
        names: dict[str, str] = {}
        with low_stock._erp_session() as source:
            c = source.cursor()
            for i in range(0, len(parts), 500):
                chunk = parts[i:i+500]
                c.execute('SELECT ProdID, ProdName FROM comProduct WHERE ProdID IN (' + ','.join(['%s']*len(chunk)) + ')', tuple(chunk))
                names.update({str(r[0]): str(r[1] or '') for r in c.fetchall()})
        _NAME_CACHE.clear()
        _NAME_CACHE[key] = names
    return _NAME_CACHE[key]


def source_stock(pairs: list[tuple[str, str]]) -> dict[tuple[str, str], float]:
    """公司3唯讀庫存，依來源倉分批查詢；缺少或負庫存視為0。"""
    stock = {(p.strip().upper(), w): 0.0 for p, w in pairs}
    with low_stock._erp_session() as source:
        cur = source.cursor()
        cur.execute('SELECT DB_NAME()')
        if cur.fetchone()[0] != 'CHIComp03':
            raise ep.PoValidationError('庫存來源不是公司3，已中止')
        for ware in sorted({w for _, w in stock}):
            parts = sorted(p for p, w in stock if w == ware)
            for i in range(0, len(parts), 500):
                chunk = parts[i:i+500]
                cur.execute('SELECT ProdID, WareID, Quantity FROM comWareAmount WITH (NOLOCK) '
                            'WHERE WareID=%s AND ProdID IN (' + ','.join(['%s']*len(chunk)) + ')', (ware, *chunk))
                for part, w, qty in cur.fetchall():
                    stock[(str(part or '').strip().upper(), str(w).strip())] = max(0, float(qty or 0))
    return stock


def plan(cur: Any, target: str, lock: bool = False) -> dict[str, Any]:
    info = pending.batch_breakdown(target)
    params = er.load_cost_params(cur, target)
    hint = ' WITH (UPDLOCK, HOLDLOCK)' if lock else ''
    stock = source_stock([(r['part'], r.get('ware_out', '001')) for b in info['batches'] for r in b['rows']])
    states = {}
    prior = {r['batch_key']: r for r in db.successful_erp_transfers(target)}
    for b in info['batches']:
        b['already'] = prior.get(b['key'])
        for r in b['rows']:
            p = r['part']
            ware = r.get('ware_out', '001')
            key = (p, ware)
            skey = (str(p).strip().upper(), ware)
            stock.setdefault(skey, 0.0)
            if key not in states:
                product = one(cur, f'SELECT * FROM comProduct{hint} WHERE ProdID=%s', (p,))
                if not product:
                    if not ep.is_test(target):
                        raise ep.PoValidationError(f'目標公司找不到料號 {p}')
                    # 測試公司缺料號時，品名改讀來源，成本視為0。
                    product = {'ProdID': p, 'ProdName': source_names(target, info['batches']).get(p, ''), 'CAvgCost': 0}
                outgoing = one(cur, f'SELECT * FROM comWareAmount{hint} WHERE ProdID=%s AND WareID=%s', (p, ware)) or {}
                incoming = one(cur, f'SELECT * FROM comWareAmount{hint} WHERE ProdID=%s AND WareID=%s', (p, '015')) or {}
                states[key] = (product, outgoing, incoming)
            product, outgoing, incoming = states[key]
            qty = 0 if b['already'] else min(r['pending'], stock[skey])
            r.update(name=ep._get(product, 'ProdName') or '', ware_out=ware, stock_src=stock[skey], stock_target=float(ep._get(outgoing, 'Quantity') or 0), transfer=qty, short=r['pending']-qty,
                     **cost_values(product, outgoing, incoming, qty, params))
            stock[skey] -= qty
            # 同一次預覽後續批次的 015 平均成本必須包含前批次。
            if qty:
                incoming = dict(incoming)
                incoming.update(Quantity=float(ep._get(incoming, 'Quantity') or 0)+qty,
                                TotalCost=float(ep._get(incoming, 'TotalCost') or 0)+r['amount'])
                states[key] = (product, outgoing, incoming)
        b['totals'] = {k: sum(r[k] for r in b['rows']) for k in ('pending', 'transfer', 'short')}
    return {**info, 'target': target, 'stock_source': 'CHIComp03', 'stock_label': '公司3', 'date': int(local_now().strftime('%Y%m%d'))}


def preview() -> dict[str, Any]:
    target, creds = resolve_target()
    conn = ep._connect_write(creds)
    try:
        ep._check_db_name(conn.cursor(), target)
        result = plan(conn.cursor(), target)
        token = digest(result)
        import time
        if len(_PREVIEWS) >= 100:
            _PREVIEWS.pop(next(iter(_PREVIEWS)))
        _PREVIEWS[token] = (time.monotonic(), json.loads(json.dumps(result)))
        return {**result, 'preview_token': token}
    except Exception as exc:
        raise ep.PoValidationError(ep._safe_message(exc, creds)) from None
    finally:
        ep._close_quietly(conn, rollback=True)


def insert(cur: Any, table: str, values: dict) -> None:
    sql, args = ep.build_insert(table, ep.table_columns(cur, table), values)
    cur.execute(sql, args)


def prev_year_month(ym: int) -> int:
    return ym - 1 if ym % 100 > 1 else (ym // 100 - 1) * 100 + 12


def upsert_year_month_avg(cur: Any, ym: int, part: str, ware: str, delta: float | None = None, **values: Any) -> None:
    """stkYearMonthAvg（BatchID=''）：delta 為 AtLastQuan 增減量，其餘欄位直接覆寫。"""
    key = (ym, part, ware, '')
    old = one(cur, 'SELECT * FROM stkYearMonthAvg WITH (UPDLOCK, HOLDLOCK) WHERE YearMonth=%s AND ProdID=%s AND WareID=%s AND BatchID=%s', key)
    if delta is not None:
        values['AtLastQuan'] = float(ep._get(old or {}, 'AtLastQuan') or 0) + delta
    if old:
        sql, args = er.build_update('stkYearMonthAvg', values, dict(YearMonth=ym, ProdID=part, WareID=ware, BatchID=''))
        cur.execute(sql, args)
    else:
        insert(cur, 'stkYearMonthAvg', er._row('stkYearMonthAvg', YearMonth=ym, ProdID=part, WareID=ware, BatchID='',
                                               OnlyCost=False, sBatchID='', **values))


def sync_company_avg(cur: Any, ym: int, product: dict, part: str) -> None:
    """CostByWare=0（公司3）：只同步當月 stkYearMonthAvg（WareID=''）；正航手開調撥不動上月。"""
    cur.execute('SELECT SUM(Quantity) FROM comWareAmount WHERE ProdID=%s', (part,))
    total = float(cur.fetchone()[0] or 0)
    upsert_year_month_avg(cur, ym, part, '', AtLastQuan=total,
                          MonthCost=float(ep._get(product, 'CAvgCost') or 0),
                          AtLastCost=float(ep._get(product, 'CTotalCost') or 0))


def ensure_masters(cur: Any, target: str, parts: list[str]) -> None:
    missing = []
    for spec in ep._master_specs('', parts, '015'):
        cur.execute(f"SELECT 1 FROM {spec['table']} WITH (UPDLOCK, HOLDLOCK) WHERE {spec['where']}", spec['params'])
        if not cur.fetchone():
            missing.append(spec)
    if missing:
        if not ep._can_copy_masters(target, ep.erp_stock.get_settings()['database']):
            raise ep.PoValidationError('正式公司缺少料號或015倉庫，已中止')
        with low_stock._erp_session() as source:
            ep._copy_masters(source.cursor(), cur, missing, target)


def execute_bill(cur: Any, target: str, batch: dict, today: int, actor: str = 'web', client_ip: str = '') -> dict[str, Any]:
    rows = [r for r in batch['rows'] if r['transfer'] > 0]
    if not rows:
        raise ep.PoValidationError('此批次沒有可調撥數量')
    ensure_masters(cur, target, [r['part'] for r in rows])
    params = er.load_cost_params(cur, target)
    # 未勾選的前批次不會真的改變015成本；以此交易實際鎖定的庫存算入庫平均。
    rows = [dict(r) for r in rows]
    products: dict[str, dict] = {}
    for r in rows:
        product = one(cur, 'SELECT * FROM comProduct WITH (UPDLOCK, HOLDLOCK) WHERE ProdID=%s', (r['part'],))
        products[r['part']] = product or {}
        outgoing = one(cur, 'SELECT * FROM comWareAmount WITH (UPDLOCK, HOLDLOCK) WHERE ProdID=%s AND WareID=%s', (r['part'], r.get('ware_out', '001'))) or {}
        incoming = one(cur, 'SELECT * FROM comWareAmount WITH (UPDLOCK, HOLDLOCK) WHERE ProdID=%s AND WareID=%s', (r['part'], '015')) or {}
        ware, part, qty = r.get('ware_out', '001'), r['part'], r['transfer']
        if float(ep._get(outgoing, 'Quantity') or 0) < qty:
            raise ep.PoValidationError(f'目標公司 {ware} 倉庫存不足：{part}（需 {qty}），已中止')
        actual = cost_values(product, outgoing, incoming, r['transfer'], params)
        if actual['cost'] != r['cost']:
            raise ep.PoValidationError('資料已變動，請重新預覽')
        r.update(actual)
    number = next_number(cur, today)
    for table, records in build_rows(number, today, rows).items():
        for record in records:
            insert(cur, table, record)
    expected = []
    for r in rows:
        for ware, sign in [(r.get('ware_out', '001'), -1), ('015', 1)]:
            where = (r['part'], ware)
            old = one(cur, 'SELECT * FROM comWareAmount WITH (UPDLOCK, HOLDLOCK) WHERE ProdID=%s AND WareID=%s', where)
            quantity = float(ep._get(old or {}, 'Quantity') or 0) + sign*r['transfer']
            if quantity < 0:
                part, qty = r['part'], r['transfer']
                raise ep.PoValidationError(f'目標公司 {ware} 倉庫存不足：{part}（需 {qty}），已中止')
            values = {'Quantity': quantity, 'BCQty': quantity}
            if params['cost_by_ware']:
                values['TotalCost'] = float(ep._get(old or {}, 'TotalCost') or 0)+sign*r['amount']
                if ware == '015':
                    values['Cost'] = r['avg']
            if old:
                sql, args = er.build_update('comWareAmount', values, dict(ProdID=r['part'], WareID=ware))
                cur.execute(sql, args)
            else:
                insert(cur, 'comWareAmount', er._row('comWareAmount', ProdID=r['part'], WareID=ware, **values))
            expected.append((r['part'], ware, quantity))
            ym = today // 100  # 沿用 erp_receive：西元 YYYYMM。
            roll = one(cur, 'SELECT * FROM StkYearMonthQty WITH (UPDLOCK, HOLDLOCK) WHERE YearMonth=%s AND ProdID=%s AND WareID=%s', (ym, *where))
            qty = float(ep._get(roll or {}, 'Quantity') or 0)+sign*r['transfer']
            if roll:
                sql, args = er.build_update('StkYearMonthQty', {'Quantity': qty}, dict(YearMonth=ym, ProdID=r['part'], WareID=ware))
                cur.execute(sql, args)
            else:
                insert(cur, 'StkYearMonthQty', er._row('StkYearMonthQty', YearMonth=ym, ProdID=r['part'], WareID=ware, Quantity=qty))
            if params['cost_by_ware']:
                upsert_year_month_avg(cur, ym, r['part'], ware, delta=sign*r['transfer'])
        if not params['cost_by_ware']:
            # CAvgCost/CTotalCost 的正航調整公式尚未確認，不更動，只同步月成本檔。
            sync_company_avg(cur, today // 100, products[r['part']], r['part'])
        cur.execute('UPDATE comProduct SET LatestIndate=%s, LatestOutDate=%s WHERE ProdID=%s', (today, today, r['part']))
    # 正航手開調撥會寫 comChangeLog（ChiStock.Move / Action=1）；UserName 與單頭 MakerID 相同。
    maker_id = ep._env('ERP_PO_MAKER_ID', ep.DEFAULT_MAKER_ID)
    insert(cur, 'comChangeLog', er._row(
        'comChangeLog', ProgID='ChiStock.Move', ChangeTime=er._ole_date(local_now()),
        IPAddr=(client_ip or '')[:15], UserName=maker_id, PKValue=number, Action=1,
        ComputerName=socket.gethostname()[:20], WinUser=(actor or 'web')[:20],
        Flag=0, OldValue='', WinLang=1, IsChiDataSet=False, ActionName=''))
    for table, col, count in [('stkMoveMain', 'MoveNO', 1), ('stkMoveSub', 'MoveNO', len(rows)), ('comProdRec', 'BillNO', len(rows))]:
        cur.execute(f'SELECT COUNT(*) FROM {table} WHERE Flag=400 AND {col}=%s', (number,))
        if int(cur.fetchone()[0]) != count:
            raise ep.PoValidationError('調撥單回讀筆數不符，已中止')
    cur.execute('SELECT SumQty FROM stkMoveMain WHERE Flag=400 AND MoveNO=%s', (number,))
    if not ep._same(cur.fetchone()[0], sum(r['transfer'] for r in rows)):
        raise ep.PoValidationError('調撥單回讀總量不符，已中止')
    for p, ware, qty in expected:
        cur.execute('SELECT Quantity FROM comWareAmount WHERE ProdID=%s AND WareID=%s', (p, ware))
        if not ep._same(cur.fetchone()[0], qty):
            raise ep.PoValidationError('庫存回讀不符，已中止')
    return dict(move_no=number, move_date=today, sum_qty=sum(r['transfer'] for r in rows),
                lines_json=json.dumps([dict(part=r['part'], qty=r['transfer'], cost=r['cost'], ware_out=r.get('ware_out', '001')) for r in rows], ensure_ascii=False))


_PREVIEWS: dict[str, tuple[float, dict]] = {}


def _safe_log(rec: dict) -> int | None:
    try:
        return db.add_erp_transfer(rec)
    except Exception:
        return None


def _write(target: str, creds: dict, key: str, expected: dict, check_full: bool = False, actor: str = 'web', client_ip: str = '') -> dict[str, Any]:
    conn = ep._connect_write(creds)
    committed = False
    try:
        cur = conn.cursor()
        ep._check_db_name(cur, target)
        for attempt in range(3):
            current = plan(cur, target, lock=True)
            batch = next((b for b in current['batches'] if b['key'] == key), None)
            original = next((b for b in expected['batches'] if b['key'] == key), None)
            if batch and batch['already']:
                raise ep.DuplicateSubmitError(f"此批次已調撥：{batch['already']['move_no']}")
            if (not batch or not original or batch['rows'] != original['rows'] or current['date'] != expected['date']
                    or (check_full and digest(current) != digest(expected))):
                raise ep.PoValidationError('資料已變動，請重新預覽')
            try:
                result = execute_bill(cur, target, batch, current['date'], actor=actor, client_ip=client_ip)
                ep._check_db_name(cur, target)
                break
            except Exception as exc:
                if not er.is_duplicate_key_error(exc) or attempt == 2:
                    raise
                conn.rollback()
        try:
            conn.commit()
            committed = True
        except Exception:
            raise ErpUnavailable(f"提交結果不確定，請先到正航確認調撥單 {result['move_no']}，勿直接重送") from None
        return result
    except Exception as exc:
        if isinstance(exc, ep.DuplicateSubmitError):
            raise
        raise ep.PoValidationError(ep._safe_message(exc, creds)) from None
    finally:
        ep._close_quietly(conn, rollback=not committed)


def create(req: dict, actor: str = 'web', client_ip: str = '') -> dict[str, Any]:
    import time
    target, creds = resolve_target()
    token = req['client_token'].strip()
    keys = list(dict.fromkeys(req['batches']))
    if not token or not keys or len(token) > 150:
        raise ep.PoValidationError('請選擇批次並提供有效的送出識別碼')
    # 重送應回 409 與單號，即使預覽已過期。
    for rec in db.successful_erp_transfers(target):
        if rec['batch_key'] in keys and (rec['client_token'] == token or rec['status'] == 'ok'):
            raise ep.DuplicateSubmitError(f"此批次已調撥：{rec['move_no']}")
    saved = _PREVIEWS.get(req['preview_token'])
    if not saved or time.monotonic()-saved[0] > 900 or saved[1]['target'] != target:
        raise ep.PoValidationError('預覽已失效，請重新預覽')
    expected = saved[1]
    ordered = [b['key'] for b in expected['batches'] if b['key'] in keys]
    if len(ordered) != len(keys):
        raise ep.PoValidationError('批次不在預覽中，請重新預覽')
    results = []
    with ep._write_lock:
        for i, key in enumerate(ordered):
            claim = 'transfer:'+target+':'+key
            if not po_duplicates.claim(claim):
                raise ep.DuplicateSubmitError('此批次正在調撥，請勿重複送出')
            batch = next(b for b in expected['batches'] if b['key'] == key)
            base = dict(batch_key=key, batch_code=batch['code'], target_db=target, client_token=token,
                        actor=actor, client_ip=client_ip)
            try:
                result = _write(target, creds, key, expected, check_full=i == 0, actor=actor, client_ip=client_ip)
                record_id = _safe_log({**base, **result, 'status': 'ok'})
                results.append(dict(batch_key=key, ok=True, id=record_id, **result))
                if record_id is None:
                    results[-1]['warning'] = '正航已寫入，但本站紀錄失敗；請先核對正航，勿重送。後續批次已停止。'
                    break
            except ep.DuplicateSubmitError:
                raise
            except Exception as exc:
                message = ep._safe_message(exc, creds)
                _safe_log({**base, 'status': 'error', 'error': message})
                results.append(dict(batch_key=key, ok=False, error=message))
                break
            finally:
                po_duplicates.release(claim)
    return dict(results=results, stopped=[k for k in ordered if k not in {r['batch_key'] for r in results}])


def recheck(record_id: int) -> dict[str, Any]:
    rec = db.get_erp_transfer(record_id)
    if not rec:
        raise ep.PoValidationError('找不到調撥紀錄')
    # SELECT 仍需依當下調撥設定驗證公司，不接受瀏覽器指定資料庫。
    target, creds = resolve_target()
    if target != rec['target_db']:
        raise ep.PoValidationError('請切回此紀錄的調撥目標公司後重新檢查')
    conn = ep._connect_write(creds)
    try:
        cur = conn.cursor()
        ep._check_db_name(cur, target)
        cur.execute('SELECT 1 FROM stkMoveMain WHERE Flag=400 AND MoveNO=%s', (rec['move_no'],))
        if not cur.fetchone() and rec['status'] == 'ok':
            db.mark_erp_transfer_deleted(record_id)
            rec['status'] = 'deleted'
        return rec
    except Exception as exc:
        raise ep.PoValidationError(ep._safe_message(exc, creds)) from None
    finally:
        ep._close_quietly(conn, rollback=True)
