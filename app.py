import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection
from datetime import datetime, timedelta
import calendar
import math
import pytz
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 時間複利戰情室 V12.0 - 多人共用版
# 長線決策大腦 ╳ 絕對紀律執行
#
# 與舊版最大差異：
#   1. 不再寫死任何人的 Google Sheet 連結／個人資料，
#      每個使用者在左側「🔗 我的試算表設定」貼上自己的連結即可。
#   2. 試算表欄位全面改為「找得到名稱就能讀」的設計，
#      不再依賴固定的儲存格位置，新手也能照範本自己建立。
#   3. 員工股／閉鎖股、信貸試算 都改成預設關閉的進階開關，
#      一般使用者完全不會看到這些區塊。
# ==========================================

st.set_page_config(page_title="時間複利戰情室", page_icon="💰", layout="wide")

# ──────────────────────────────────────────
# ① 全域常數（CONFIG）－ 只放「預設值」，不放任何人的個人資料
# ──────────────────────────────────────────
class CONFIG:
    PRICE_TTL = 60                  # 報價 cache 秒數

    DEFAULT_TW_TICKER     = "00631L"
    DEFAULT_SPLIT_CUTOFF  = pd.to_datetime("2026-03-23")
    DEFAULT_SPLIT_RATIO   = 22.0
    DEFAULT_SPLIT_THRESH  = 100.0

    DEFAULT_BASE_M_WAN    = 10.0
    DEFAULT_CASH_WAN      = 200.0
    DEFAULT_TARGET_WAN    = 2000.0  # 人生目標資產（萬）

    # 找不到使用者自訂槓桿倍數時的備援表（常見槓桿/非槓桿 ETF）
    DEFAULT_LEVERAGE_MAP = {
        "SOXL": 3, "TQQQ": 3, "UPRO": 3, "SPXL": 3, "TMF": 0,
        "BITX": 2, "QQQM": 1, "QQQ": 1, "SPY": 1, "VOO": 1, "BOXX": 0,
    }

    # 狙擊表：(單日跌幅門檻, 倍數, 標籤)
    SNIPER_TABLE = [
        (-15.0, 4.0, "🔴 重壓 (4.0x)"),
        (-10.0, 3.0, "🔴 恐慌買 (3.0x)"),
        ( -8.0, 2.0, "🟠 恐慌買 (2.0x)"),
        ( -6.0, 1.5, "🟠 中型修正 (1.5x)"),
        ( -5.0, 1.0, "🟡 標準買點 (1.0x)"),
        ( -4.0, 0.5, "🟡 波段低接 (0.5x)"),
        ( -3.0, 0.25,"🟢 日常試單 (0.25x)"),
    ]

TW_TZ = pytz.timezone("Asia/Taipei")

# ──────────────────────────────────────────
# ② 工具函式層
# ──────────────────────────────────────────

def to_float(val) -> float:
    """任意值安全轉 float，失敗回 0.0"""
    try:
        return float(str(val).replace(",", "").replace("$", "").replace("%", "").strip())
    except Exception:
        return 0.0


def find_col(df: pd.DataFrame, *keywords) -> str | None:
    """在欄位名稱中尋找含有任一關鍵字的欄位（找不到回傳 None）"""
    for c in df.columns:
        cs = str(c)
        if any(k in cs for k in keywords):
            return c
    return None


def parse_kv_settings(df: pd.DataFrame) -> dict:
    """讀取『參數名稱／數值』兩欄式設定表，回傳 {參數名稱: 數值}"""
    if df.empty:
        return {}
    key_col = find_col(df, "參數", "項目", "設定") or (df.columns[0] if len(df.columns) > 0 else None)
    val_col = find_col(df, "數值", "金額", "值")   or (df.columns[1] if len(df.columns) > 1 else None)
    if key_col is None or val_col is None:
        return {}
    out = {}
    for _, row in df.iterrows():
        k = str(row.get(key_col, "")).strip()
        if k and k.lower() != "nan":
            out[k] = row.get(val_col, "")
    return out


def get_kv_float(kv: dict, *keys, default: float = 0.0) -> float:
    for need in keys:
        for actual in kv:
            if need in actual:
                return to_float(kv[actual])
    return default


def get_kv_str(kv: dict, *keys, default: str = "") -> str:
    for need in keys:
        for actual in kv:
            if need in actual:
                v = str(kv[actual]).strip()
                if v and v.lower() != "nan":
                    return v
    return default


def parse_etf_leverage(df: pd.DataFrame) -> dict:
    """讀取『ETF代號／槓桿倍數』表，回傳 {代號: 倍數}"""
    out = {}
    if df.empty:
        return out
    col_t = find_col(df, "代號", "代碼")
    col_l = find_col(df, "槓桿")
    if col_t is None:
        return out
    for _, row in df.iterrows():
        t = str(row.get(col_t, "")).strip().upper()
        if not t or t == "NAN":
            continue
        out[t] = to_float(row.get(col_l, 1)) if col_l else 1.0
    return out


def make_split_adjuster(enabled: bool, cutoff, ratio: float, threshold: float):
    """回傳一個價格還原函式：enabled=False 時原封不動回傳"""
    def _adj(price: float) -> float:
        if not enabled:
            return round(price, 2)
        return round(price / ratio, 2) if price > threshold else round(price, 2)
    return _adj


def next_first_wednesday(from_date: datetime.date) -> datetime.date:
    """計算下一個（或當月）首個週三"""
    today = from_date
    cal = calendar.monthcalendar(today.year, today.month)
    first_wed = cal[0][2] if cal[0][2] != 0 else cal[1][2]
    dca = datetime(today.year, today.month, first_wed).date()
    if today > dca:
        m = today.month + 1 if today.month < 12 else 1
        y = today.year if today.month < 12 else today.year + 1
        cal2 = calendar.monthcalendar(y, m)
        fw2 = cal2[0][2] if cal2[0][2] != 0 else cal2[1][2]
        dca = datetime(y, m, fw2).date()
    return dca


def sniper_signal(daily_pct: float) -> tuple[float, str]:
    """根據單日跌幅回傳 (倍數, 標籤)，無觸發回 (0, '保留現金')"""
    for thresh, mult, label in CONFIG.SNIPER_TABLE:
        if daily_pct <= thresh:
            return mult, label
    return 0.0, "保留現金"


def get_tw_session_label() -> str:
    """根據台灣時間判斷台股目前交易時段"""
    import datetime as dt_mod
    now = datetime.now(TW_TZ)
    t  = now.time()
    wd = now.weekday()
    if wd >= 5:
        return "🌙 週末休市"
    pre_open  = dt_mod.time(8, 0)
    open_t    = dt_mod.time(9, 0)
    close_t   = dt_mod.time(13, 30)
    after_t   = dt_mod.time(14, 30)
    if pre_open <= t < open_t:
        return "🌅 盤前"
    if open_t <= t < close_t:
        return "☀️ 盤中"
    if close_t <= t < after_t:
        return "🌆 盤後"
    return "🌙 休市"


# ──────────────────────────────────────────
# ③ 資料擷取層（帶 cache）
# ──────────────────────────────────────────

@st.cache_data(ttl=CONFIG.PRICE_TTL)
def fetch_tw_price(ticker: str, fugle_key: str = "") -> dict:
    """回傳 dict: curr, prev, source, time_str, age_min, session。優先 Fugle → yfinance"""
    if fugle_key:
        try:
            from fugle_marketdata import RestClient
            client = RestClient(api_key=fugle_key)
            q = client.stock.intraday.quote(symbol=ticker)
            curr = q.get("closePrice") or q.get("lastPrice") or q.get("referencePrice", 0)
            prev = q.get("referencePrice", curr)
            raw_t = q.get("lastUpdated") or q.get("lastTrade", {}).get("time")
            time_str, age_min = _parse_fugle_time(raw_t)
            return dict(curr=float(curr), prev=float(prev), source="🟢 Fugle",
                        time_str=time_str, age_min=age_min, session=get_tw_session_label())
        except ImportError:
            pass
        except Exception:
            pass

    yf_sym = ticker + ".TW"

    try:
        tkr = yf.Ticker(yf_sym)
        fi = tkr.fast_info
        curr = float(fi.last_price)
        prev = float(fi.previous_close)
        try:
            ts = fi.regular_market_time
            dt = (datetime.fromtimestamp(ts, tz=TW_TZ) if isinstance(ts, (int, float))
                  else ts.astimezone(TW_TZ))
            age_min = (datetime.now(TW_TZ) - dt).total_seconds() / 60
            time_str = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            age_min, time_str = 999, "無法取得"
        return dict(curr=curr, prev=prev, source="🟡 yfinance fast_info",
                    time_str=time_str, age_min=age_min, session=get_tw_session_label())
    except Exception:
        pass

    try:
        hist = yf.download(yf_sym, period="5d", progress=False)
        closes = (hist["Close"][yf_sym] if isinstance(hist.columns, pd.MultiIndex)
                  else hist["Close"]).dropna()
        curr = float(closes.iloc[-1])
        prev = float(closes.iloc[-2]) if len(closes) >= 2 else curr
        return dict(curr=curr, prev=prev, source="🔴 yfinance 歷史備援",
                    time_str="歷史資料", age_min=9999, session=get_tw_session_label())
    except Exception:
        return dict(curr=0.0, prev=0.0, source="❌ 完全失敗", time_str="N/A", age_min=99999, session="❓")


def _parse_fugle_time(raw_t) -> tuple[str, float]:
    try:
        if isinstance(raw_t, (int, float)):
            unit = "us" if raw_t > 1e14 else ("ms" if raw_t > 1e11 else "s")
            dt = pd.to_datetime(raw_t, unit=unit, utc=True).astimezone(TW_TZ)
        else:
            dt = pd.to_datetime(raw_t)
            dt = dt.tz_localize("Asia/Taipei") if dt.tzinfo is None else dt.astimezone(TW_TZ)
        age_min = (datetime.now(TW_TZ) - dt).total_seconds() / 60
        return dt.strftime("%Y-%m-%d %H:%M"), age_min
    except Exception:
        return "未知", 0.0


def _get_us_session_label(now_et) -> str:
    t = now_et.time()
    import datetime as dt_mod
    pre  = (dt_mod.time(4, 0), dt_mod.time(9, 30))
    reg  = (dt_mod.time(9, 30), dt_mod.time(16, 0))
    post = (dt_mod.time(16, 0), dt_mod.time(20, 0))
    wd = now_et.weekday()
    if wd >= 5:
        return "🌙 週末休市"
    if pre[0] <= t < pre[1]:
        return "🌅 盤前"
    if reg[0] <= t < reg[1]:
        return "☀️ 盤中"
    if post[0] <= t < post[1]:
        return "🌆 盤後"
    return "🌙 休市"


@st.cache_data(ttl=CONFIG.PRICE_TTL)
def fetch_us_price(ticker: str) -> dict:
    import pytz as _pytz, datetime as dt_mod
    et_tz = _pytz.timezone("America/New_York")
    now_et = dt_mod.datetime.now(et_tz)
    session = _get_us_session_label(now_et)

    try:
        tkr = yf.Ticker(ticker)
        hist = tkr.history(period="2d", interval="1m", prepost=True)
        if not hist.empty:
            curr = float(hist["Close"].iloc[-1])
            prev = float(tkr.fast_info.previous_close)
            last_time = hist.index[-1].astimezone(et_tz)
            time_str = last_time.strftime("%Y-%m-%d %H:%M ET")
            return dict(curr=curr, prev=prev, session=session, source="🟢 yfinance", time_str=time_str)
    except Exception:
        pass

    return dict(curr=0.0, prev=0.0, session="❓", source="❌ 完全失敗", time_str="N/A")


def read_gsheets(conn, url: str, worksheet: str | None = None, **kwargs) -> pd.DataFrame:
    """安全讀取 Google Sheets 指定分頁，失敗回空 DataFrame（不中斷整體流程）"""
    if not url:
        return pd.DataFrame()
    try:
        if worksheet:
            df = conn.read(spreadsheet=url, worksheet=worksheet, ttl=0, **kwargs)
        else:
            df = conn.read(spreadsheet=url, ttl=0, **kwargs)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        st.sidebar.warning(f"⚠️ 讀取分頁「{worksheet or '預設'}」失敗：{e}")
        return pd.DataFrame()


def calculate_loan(principal: float, annual_rate_pct: float,
                   years: int, start_date: datetime.date) -> tuple[float, float]:
    """回傳 (剩餘本金, 每月還款額)"""
    if principal <= 0 or years <= 0:
        return 0.0, 0.0
    r  = annual_rate_pct / 100 / 12
    N  = years * 12
    pmt = (principal * r * (1+r)**N / ((1+r)**N - 1)) if r > 0 else (principal / N)
    today = datetime.today().date()
    passed = (today.year - start_date.year) * 12 + (today.month - start_date.month)
    if today.day >= start_date.day:
        passed += 1
    passed = max(0, min(passed, int(N)))
    rem = (principal * ((1+r)**N - (1+r)**passed) / ((1+r)**N - 1)) if r > 0 else max(0, principal - pmt * passed)
    return max(0.0, rem), pmt


# ──────────────────────────────────────────
# ④ 業務邏輯層
# ──────────────────────────────────────────

def parse_tw_trades(df_raw: pd.DataFrame) -> dict:
    """解析台股交易紀錄（欄位：交易日期／交易類型／成交股數／成交價格）"""
    result = dict(shares=0.0, cost=0.0, min_date=pd.NaT, raw_buys=pd.DataFrame())
    if df_raw.empty:
        return result
    col_date   = find_col(df_raw, "日期")
    col_type   = find_col(df_raw, "類型")
    col_shares = find_col(df_raw, "股數")
    col_price  = find_col(df_raw, "價格", "成交價")
    if not all([col_date, col_type, col_shares, col_price]):
        return result

    df = df_raw.copy()
    df["_date"] = pd.to_datetime(df[col_date], errors="coerce")
    df = df.dropna(subset=["_date"])
    if df.empty:
        return result
    df["_shares"] = pd.to_numeric(df[col_shares].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["_price"]  = pd.to_numeric(df[col_price].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["_cost"]   = df["_shares"] * df["_price"]
    is_sell = df[col_type].astype(str).str.contains("賣出", na=False)
    df.loc[is_sell, ["_shares", "_cost"]] *= -1

    result["shares"]   = df["_shares"].sum()
    result["cost"]     = df["_cost"].sum()
    result["min_date"] = df["_date"].min()

    buys = df[~is_sell].copy()
    buys["成交日期"] = buys["_date"]
    buys["庫存股數"] = buys["_shares"]
    buys["持有成本"] = buys["_cost"]
    buys["成交價格"] = buys["_price"]
    result["raw_buys"] = buys
    return result


def parse_us_trades(df_raw: pd.DataFrame, ticker: str) -> dict:
    """解析單一美股代號的交易紀錄（欄位：交易日期／股票代號／交易類型／成交股數／成交價格）"""
    result = dict(shares=0.0, cost=0.0, first_date=pd.NaT)
    if df_raw.empty:
        return result
    col_ticker = find_col(df_raw, "代號", "代碼")
    col_date   = find_col(df_raw, "日期")
    col_type   = find_col(df_raw, "類型")
    col_shares = find_col(df_raw, "股數")
    col_price  = find_col(df_raw, "價格", "成交價")
    if not all([col_ticker, col_date, col_type, col_shares, col_price]):
        return result

    df = df_raw[df_raw[col_ticker].astype(str).str.upper().str.strip() == ticker.upper()].copy()
    if df.empty:
        return result
    df["_date"] = pd.to_datetime(df[col_date], errors="coerce")
    df = df.dropna(subset=["_date"])
    if df.empty:
        return result
    df["_shares"] = pd.to_numeric(df[col_shares].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["_price"]  = pd.to_numeric(df[col_price].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["_cost"]   = df["_shares"] * df["_price"]
    is_sell = df[col_type].astype(str).str.contains("賣出", na=False)
    df.loc[is_sell, ["_shares", "_cost"]] *= -1

    result["shares"]     = df["_shares"].sum()
    result["cost"]       = df["_cost"].sum()
    result["first_date"] = df["_date"].min()
    return result


def parse_soxl_grid(df_raw: pd.DataFrame) -> dict:
    """解析『網格策略』分頁：預估股價／預估股數／實際股數／實際成本價／實際停利股價／停利%"""
    empty = dict(tranche_no=0, total_shares=0, avg_price=0, tp_price=0,
                 tp_pct=0, next_add_price=0, next_add_shares=0)
    if df_raw.empty:
        return empty

    col_k = find_col(df_raw, "實際股數")
    col_l = find_col(df_raw, "實際成本價")
    col_m = find_col(df_raw, "實際停利股價")
    col_d = find_col(df_raw, "預估股價")
    col_e = find_col(df_raw, "預估股數")
    col_g = find_col(df_raw, "停利%", "停利")

    if col_k is None:
        return empty

    df = df_raw.copy()
    df["_K"] = pd.to_numeric(df[col_k].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce").fillna(0)

    if col_d:
        df["_D"] = pd.to_numeric(df[col_d].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce").fillna(0)
        valid_df = df[df["_D"] > 0].reset_index(drop=True)
    else:
        valid_df = df.reset_index(drop=True)

    active_df = valid_df[valid_df["_K"] > 0]

    result = dict(**empty)
    if not active_df.empty:
        last_row = valid_df.iloc[active_df.index[-1]]
        result["tranche_no"]   = len(active_df)
        result["total_shares"] = active_df["_K"].sum()
        if col_l:
            result["avg_price"] = to_float(last_row[col_l])
        if col_m:
            result["tp_price"] = to_float(last_row[col_m])
        if col_g:
            raw = to_float(last_row[col_g])
            result["tp_pct"] = raw * 100 if raw < 10 else raw
        next_idx = active_df.index[-1] + 1
        if next_idx < len(valid_df):
            nr = valid_df.iloc[next_idx]
            result["next_add_price"]  = to_float(nr[col_d]) if col_d else 0
            result["next_add_shares"] = to_float(nr[col_e]) if col_e else 0
    else:
        if not valid_df.empty:
            fr = valid_df.iloc[0]
            result["next_add_price"]  = to_float(fr[col_d]) if col_d else 0
            result["next_add_shares"] = to_float(fr[col_e]) if col_e else 0
    return result


def parse_cash_parking(df_raw: pd.DataFrame) -> list[dict]:
    """解析『資金停泊』分頁：停泊類型／金額(USD)／到期日／備註"""
    result = []
    if df_raw.empty:
        return result

    col_type = find_col(df_raw, "停泊類型", "類型")
    col_amt  = find_col(df_raw, "金額")
    col_mat  = find_col(df_raw, "到期")
    col_note = find_col(df_raw, "備註")

    if col_type is None or col_amt is None:
        return result

    today = datetime.today().date()
    for _, row in df_raw.iterrows():
        t = str(row.get(col_type, "")).strip()
        if t in ("", "nan", "None") or t not in ("CD", "T-Bill", "國債"):
            continue
        amt = to_float(row.get(col_amt, 0))
        if amt <= 0:
            continue
        mat_raw = row.get(col_mat, "")
        mat_date, days_left = None, None
        try:
            mat_date  = pd.to_datetime(mat_raw).date()
            days_left = (mat_date - today).days
        except Exception:
            pass
        note = str(row.get(col_note, "")).strip() if col_note else ""
        result.append(dict(type=t, amount_usd=amt, maturity=mat_date, days_left=days_left, note=note))
    return result


def compute_portfolio(tw_trade: dict, us_live: dict,
                      p_tw_curr: float, p_tw_yest: float,
                      cash_twd: float, loan_twd: float,
                      us_cash_usd: float, usd_twd: float,
                      cash_parking: list | None = None,
                      leverage_map: dict | None = None) -> dict:
    """彙整雙帳戶資產、曝險度。所有台幣金額後綴 _twd，美元後綴 _usd。"""
    leverage_map = leverage_map or {}

    val_tw_twd  = tw_trade["shares"] * p_tw_curr
    cost_tw_twd = tw_trade["cost"]
    exp_tw_twd  = val_tw_twd * 2
    fc_tw_twd   = val_tw_twd + cash_twd - loan_twd
    pct_tw      = (exp_tw_twd / fc_tw_twd * 100) if fc_tw_twd > 0 else 0
    daily_pnl_twd = (p_tw_curr - p_tw_yest) * tw_trade["shares"]
    roi_tw      = (val_tw_twd / cost_tw_twd - 1) if cost_tw_twd > 0 else 0

    val_us_usd  = sum(v["shares"] * v["curr"] for v in us_live.values())
    cost_us_usd = sum(v["cost"]   for v in us_live.values())
    exp_us_usd  = sum(v["shares"] * v["curr"] * leverage_map.get(t, 1) for t, v in us_live.items())
    cd_total_usd = sum(p["amount_usd"] for p in (cash_parking or []))
    fc_us_usd   = val_us_usd + us_cash_usd + cd_total_usd
    pct_us      = (exp_us_usd / fc_us_usd * 100) if fc_us_usd > 0 else 0
    daily_pnl_usd = sum((v["curr"] - v["yest"]) * v["shares"] for v in us_live.values())
    us_roi      = (val_us_usd / cost_us_usd - 1) if cost_us_usd > 0 else 0

    fc_total_twd  = fc_tw_twd + fc_us_usd * usd_twd
    exp_total_twd = exp_tw_twd + exp_us_usd * usd_twd
    pct_total     = (exp_total_twd / fc_total_twd * 100) if fc_total_twd > 0 else 0

    return dict(
        val_tw_twd=val_tw_twd, cost_tw_twd=cost_tw_twd,
        exp_tw_twd=exp_tw_twd, fc_tw_twd=fc_tw_twd,
        pct_tw=pct_tw, daily_pnl_twd=daily_pnl_twd, roi_tw=roi_tw,
        val_us_usd=val_us_usd, cost_us_usd=cost_us_usd,
        exp_us_usd=exp_us_usd, fc_us_usd=fc_us_usd,
        pct_us=pct_us, daily_pnl_usd=daily_pnl_usd, us_roi=us_roi,
        fc_total_twd=fc_total_twd, exp_total_twd=exp_total_twd, pct_total=pct_total,
    )

# ──────────────────────────────────────────
# ⑤ UI 元件層
# ──────────────────────────────────────────

def render_price_freshness(source: str, time_str: str, age_min: float, session: str = ""):
    session_tag = f" {session}" if session else ""
    if age_min < 60:
        st.caption(f"{source}{session_tag} {time_str}（{age_min:.0f} 分鐘前）")
    elif age_min < 9999:
        st.caption(f"{source}{session_tag} {time_str}（{age_min/60:.1f} 小時前）")
    else:
        st.caption(f"{source}{session_tag} ｜ 使用歷史收盤價（{time_str}）")


def render_onboarding_tw():
    st.info(
        "👋 **尚未設定台股帳本連結**\n\n"
        "1️⃣ 用「台股帳本範本.xlsx」在 Google Sheets 建立你自己的試算表"
        "（檔案 → 匯入 → 上傳，選擇「插入新試算表」）\n\n"
        "2️⃣ 把共用權限改成「知道連結的人皆可檢視」\n\n"
        "3️⃣ 把連結貼到左側「🔗 我的試算表設定」的『台股帳本連結』欄位"
    )


def render_onboarding_us():
    st.info(
        "👋 **尚未設定美股帳本連結**\n\n"
        "1️⃣ 用「美股帳本範本.xlsx」在 Google Sheets 建立你自己的試算表\n\n"
        "2️⃣ 把共用權限改成「知道連結的人皆可檢視」\n\n"
        "3️⃣ 把連結貼到左側「🔗 我的試算表設定」的『美股帳本連結』欄位"
    )


def render_tab_tw(tw_trade: dict, port: dict, p_tw_curr: float, p_tw_yest: float,
                  base_m: float, loan1: float, loan2: float, cash_twd: float,
                  tw_price: dict | None = None, tw_ticker: str = "00631L",
                  loan_enabled: bool = False, tw_url: str = ""):
    if tw_price:
        render_price_freshness(tw_price["source"], tw_price["time_str"], tw_price["age_min"], tw_price.get("session", ""))
    shares = tw_trade["shares"]
    cost   = port["cost_tw_twd"]
    val    = port["val_tw_twd"]
    roi    = port["roi_tw"]
    min_date = tw_trade["min_date"]

    days = max((datetime.today() - min_date).days, 1) if pd.notnull(min_date) else 1
    ann_roi = ((1 + roi) ** (365 / days) - 1) * 100
    daily_pct = (p_tw_curr / p_tw_yest - 1) * 100 if p_tw_yest > 0 else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("市值",       f"{val/10000:,.0f} 萬")
    c2.metric("成本",       f"{cost/10000:,.0f} 萬")
    c3.metric("未實現損益", f"{(val-cost)/10000:+,.0f} 萬", f"{roi*100:+.1f}%")
    c4.metric("今日損益",   f"{port['daily_pnl_twd']:+,.0f}", f"{daily_pct:+.2f}%")
    c5.metric("曝險度",     f"{port['pct_tw']:.1f}%")

    c6, c7, c8, c9, c10 = st.columns(5)
    c6.metric("庫存張數",   f"{shares/1000:,.0f} 張")
    c7.metric("均價",       f"{cost/shares:.2f}" if shares > 0 else "0")
    c8.metric("昨日收盤",   f"{p_tw_yest:.2f}")
    c9.metric("目前現價",   f"{p_tw_curr:.2f}")
    c10.metric("年化報酬",  f"{ann_roi:+.2f}%")

    st.divider()
    st.subheader("🚨 雙引擎戰略")
    roi_pct = roi * 100

    if roi_pct >= 0:
        adj_pct = min(roi_pct, 20.0)
        dynamic_m = max(base_m * (1 - adj_pct/100), base_m * 0.8)
        adj_str = f"降 {adj_pct:.1f}% (獲利調節)"
    else:
        adj_pct = min(abs(roi_pct) * 2, 100.0)
        dynamic_m = min(base_m * (1 + adj_pct/100), base_m * 2.0)
        adj_str = f"升 {adj_pct:.1f}% (虧損加碼)"

    today_d  = datetime.today().date()
    dca_date = next_first_wednesday(today_d)
    is_dca   = (today_d == dca_date)

    sniper_mult, sniper_label = sniper_signal(daily_pct)
    sniper_m = dynamic_m * sniper_mult

    if is_dca and sniper_m > 0:
        final_amt, action_label = max(dynamic_m, sniper_m), "🔥 定額與狙擊撞日 (擇高投入)"
    elif is_dca:
        final_amt, action_label = dynamic_m, "📅 執行每月動態定額"
    elif sniper_m > 0:
        final_amt, action_label = sniper_m, f"🎯 執行階梯狙擊 ({sniper_label})"
    else:
        final_amt, action_label = 0, "觀望不動"

    st.info("💡 **資金鐵則：** 帳戶請隨時鎖定一定倍數的現金流，作為戰略預備金。")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("#### 📅 引擎一：動態定額")
        st.write(f"**下次回款日：** {dca_date} {'(🟢 今日!)' if is_dca else ''}")
        st.write(f"**庫存總損益：** {roi_pct:+.2f}%")
        st.write(f"**調整幅度：** {adj_str}")
        st.metric("當月動態基準", f"NT$ {dynamic_m:,.0f}")
    with col2:
        st.markdown("#### 🎯 引擎二：階梯狙擊")
        st.write(f"**今日漲跌幅：** {daily_pct:+.2f}%")
        st.write(f"**觸發位階：** {sniper_label}")
        st.write("**加碼公式：** 動態基準 × 倍數")
        st.metric("今日狙擊金額", f"NT$ {sniper_m:,.0f}")
    with col3:
        st.markdown("#### 🚀 今日最終行動指示")
        if final_amt > 0:
            st.success(f"**{action_label}**")
            st.metric("建議投入本金", f"NT$ {final_amt:,.0f}")
            st.metric("換算購買股數", f"約 {(final_amt/p_tw_curr):,.0f} 股" if p_tw_curr > 0 else "0 股")
        else:
            st.warning("☕ 目前未達狙擊標準且非扣款日，請保留現金觀望。")

    st.divider()
    col_p, col_d = st.columns([2, 1])
    with col_p:
        st.write("📊 **台幣資產與淨值變動 (瀑布圖)**")
        labels   = [f"{tw_ticker} 市值", "可用現金"]
        measures = ["relative", "relative"]
        values   = [val, cash_twd]
        if loan_enabled and (loan1 + loan2) > 0:
            labels.append("信貸總餘額"); measures.append("relative"); values.append(-(loan1 + loan2))
        net = val + cash_twd - (loan1 + loan2 if loan_enabled else 0)
        labels.append(f"{tw_ticker} 獨立淨資產"); measures.append("total"); values.append(net)
        fig = go.Figure(go.Waterfall(
            orientation="v", x=labels, measure=measures, y=values,
            textposition="inside", texttemplate="NT$ %{y:,.0f}",
            textfont=dict(color="black", size=13),
            increasing={"marker": {"color": "#2EC4B6"}},
            decreasing={"marker": {"color": "#E71D36"}},
            totals={"marker": {"color": "#FF9F1C"}},
            connector={"line": {"color": "#5C5C5C", "width": 1, "dash": "dot"}},
        ))
        fig.update_layout(height=380, margin=dict(l=10, r=10, t=40, b=10),
                          showlegend=False, yaxis=dict(title="金額 (NT$)"))
        st.plotly_chart(fig, use_container_width=True)
    with col_d:
        st.info(f"💡 **{tw_ticker} 獨立淨資產**\n\nNT$ {port['fc_tw_twd']/10000:,.1f} 萬\n\n*市值 + 台幣現金{'－總信貸' if loan_enabled else ''}*")

    with st.expander(f"📜 逐筆投資戰績表 (目前現價: {p_tw_curr:.2f})", expanded=False):
        buy_df = tw_trade.get("raw_buys", pd.DataFrame())
        if not buy_df.empty:
            recs = []
            for _, r in buy_df.sort_values("成交日期", ascending=False).iterrows():
                adj_p, adj_s = r["成交價格"], r["庫存股數"]
                l_pnl = adj_s * p_tw_curr - r["持有成本"]
                l_roi = l_pnl / r["持有成本"] if r["持有成本"] > 0 else 0
                days_held = max((datetime.today() - r["成交日期"]).days, 1)
                l_ann = ((1 + l_roi) ** (365 / days_held) - 1) * 100
                recs.append({
                    "日期": r["成交日期"].strftime("%Y-%m-%d"),
                    "買價": f"{adj_p:.2f}", "股數": f"{adj_s:,.0f}",
                    "目前現價": f"{p_tw_curr:.2f}",
                    "今日損益": f"{(p_tw_curr - p_tw_yest) * adj_s:+,.0f}",
                    "總損益": f"{l_pnl:+,.0f}", "年化報酬": f"{l_ann:+.1f}%",
                    "總報酬": f"{l_roi*100:+.1f}%",
                })
            st.dataframe(pd.DataFrame(recs), use_container_width=True, hide_index=True)
        else:
            st.caption("尚無買入紀錄。")

    st.subheader("🌐 戰術圖表分析")
    _render_tw_charts(tw_trade, p_tw_curr, p_tw_yest, tw_ticker)

    st.divider()
    if tw_url:
        st.link_button("🛒 新增台股交易紀錄 (Google Sheets)", tw_url, use_container_width=True)


def _render_tw_charts(tw_trade: dict, p_tw_curr: float, p_tw_yest: float, tw_ticker: str):
    try:
        yf_sym = f"{tw_ticker}.TW"
        hist = yf.download(yf_sym, period="5y", progress=False)
        if hist.empty:
            st.caption("查無歷史價格資料。")
            return
        raw_close = hist["Close"][yf_sym] if isinstance(hist.columns, pd.MultiIndex) else hist["Close"]
        adj = raw_close.copy()

        min_date = tw_trade["min_date"]
        start    = min_date if pd.notnull(min_date) else pd.to_datetime("2024-01-01")
        rp       = adj[adj.index >= start]
        if rp.dropna().empty:
            return

        avg_cost = tw_trade["cost"] / tw_trade["shares"] if tw_trade["shares"] > 0 else 0

        st.write("📈 **A. 價格走勢與還原均價**")
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=rp.index, y=rp.values, name="還原價", line=dict(color="#E71D36")))
        mx, mi, lt = rp.max(), rp.min(), rp.dropna().iloc[-1]
        if avg_cost > 0:
            fig1.add_hline(y=avg_cost, line_dash="dash", line_color="#00A86B",
                           annotation_text=f"🟢 均價線: {avg_cost:.2f}")
            fig1.add_hrect(y0=avg_cost, y1=max(mx*1.1, avg_cost*1.1), fillcolor="green", opacity=0.1, layer="below", line_width=0)
            fig1.add_hrect(y0=min(mi*0.9, avg_cost*0.9), y1=avg_cost, fillcolor="red", opacity=0.1, layer="below", line_width=0)
        fig1.add_annotation(x=rp.idxmax(), y=mx, text=f"高:{mx:.2f}", showarrow=True, ay=-30)
        fig1.add_annotation(x=rp.idxmin(), y=mi, text=f"低:{mi:.2f}", showarrow=True, ay=30)
        fig1.add_annotation(x=rp.index[-1], y=lt, text=f"最新:{lt:.2f}", showarrow=True, ax=40)
        y0 = min(mi*0.9, avg_cost*0.9) if avg_cost > 0 else mi*0.9
        y1 = max(mx*1.1, avg_cost*1.1) if avg_cost > 0 else mx*1.1
        fig1.update_yaxes(range=[y0, y1])
        st.plotly_chart(fig1, use_container_width=True)

        st.write("📊 **B. 多空戰術乖離率**")
        bias = (rp - rp.rolling(20).mean()) / rp.rolling(20).mean() * 100
        bc = bias.dropna()
        if not bc.empty:
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=bc.index, y=bc.values, name="乖離%", line=dict(color="#F4A261")))
            for v, c, t in [(-5, "gray", "標準(-5)"), (-10, "orange", "恐慌(-10)"), (-15, "red", "重壓(-15)")]:
                fig2.add_hline(y=v, line_dash="dot", line_color=c, annotation_text=t)
            bx, bi, bl = bc.max(), bc.min(), bc.iloc[-1]
            fig2.add_hrect(y0=0, y1=max(bx*1.2, 10), fillcolor="green", opacity=0.1, layer="below", line_width=0)
            fig2.add_hrect(y0=min(bi*1.2, -20), y1=0, fillcolor="red", opacity=0.1, layer="below", line_width=0)
            fig2.add_annotation(x=bc.idxmax(), y=bx, text=f"最高:{bx:.1f}%", showarrow=True, ay=-30)
            fig2.add_annotation(x=bc.idxmin(), y=bi, text=f"最低:{bi:.1f}%", showarrow=True, ay=30)
            fig2.add_annotation(x=bc.index[-1], y=bl, text=f"最新:{bl:.1f}%", showarrow=True, ax=40)
            fig2.update_yaxes(range=[min(bi*1.2, -20), max(bx*1.2, 15)])
            st.plotly_chart(fig2, use_container_width=True)

        st.write("💰 **C. 庫存真實損益軌跡**")
        buy_df = tw_trade.get("raw_buys", pd.DataFrame())
        if not buy_df.empty:
            th = buy_df.groupby("成交日期")[["庫存股數", "持有成本"]].sum().reindex(rp.index).fillna(0)
            ds = th["庫存股數"].cumsum()
            dc = th["持有成本"].cumsum()
            dp = np.where(dc > 0, (ds * rp - dc) / dc * 100, 0)
            dp_s = pd.Series(dp, index=rp.index).dropna()
            if not dp_s.empty:
                px, pi, pl = dp_s.max(), dp_s.min(), dp_s.iloc[-1]
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=dp_s.index, y=dp_s.values, line=dict(color="#247BA0")))
                fig3.add_hrect(y0=0, y1=max(px*1.2, 10), fillcolor="green", opacity=0.1, layer="below", line_width=0)
                fig3.add_hrect(y0=min(pi*1.2, -10), y1=0, fillcolor="red", opacity=0.1, layer="below", line_width=0)
                fig3.add_annotation(x=dp_s.idxmax(), y=px, text=f"最高:{px:.1f}%", showarrow=True, ay=-30)
                fig3.add_annotation(x=dp_s.idxmin(), y=pi, text=f"最低:{pi:.1f}%", showarrow=True, ay=30)
                fig3.add_annotation(x=dp_s.index[-1], y=pl, text=f"最新:{pl:.1f}%", showarrow=True, ax=40)
                fig3.update_yaxes(range=[min(pi*1.2, -15), max(px*1.2, 20)])
                st.plotly_chart(fig3, use_container_width=True)

            st.write("💴 **D. 庫存成本 vs 市值 金額軌跡**")
            mv_m = (ds * rp) / 1_000_000
            cc_m = dc.reindex(rp.index).ffill() / 1_000_000
            mv_m = mv_m.dropna()
            cc_m = cc_m.reindex(mv_m.index)

            true_cost_m = tw_trade["cost"] / 1_000_000
            true_val_m  = tw_trade["shares"] * p_tw_curr / 1_000_000
            last_pnl = true_val_m - true_cost_m
            sign = "+" if last_pnl >= 0 else ""

            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=cc_m.index, y=cc_m.values, name="累積成本", line=dict(color="#888888", width=2)))
            fig4.add_trace(go.Scatter(x=mv_m.index, y=mv_m.values, name="市值", line=dict(color="#2EC4B6", width=2.5)))
            fig4.add_annotation(x=mv_m.idxmax(), y=mv_m.max(), text=f"最高:{mv_m.max():.2f}M", showarrow=True, ay=-30)
            fig4.add_annotation(x=mv_m.index[-1], y=mv_m.iloc[-1], text=f"最新:{true_val_m:.2f}M", showarrow=True, ax=40)
            fig4.add_annotation(x=cc_m.index[-1], y=cc_m.iloc[-1], text=f"成本:{true_cost_m:.2f}M", showarrow=True, ay=30, ax=40)
            st.plotly_chart(fig4, use_container_width=True)

            hist_pnl_m   = (mv_m - cc_m)[cc_m > 0]
            if not hist_pnl_m.empty:
                max_pnl_m    = hist_pnl_m.max()
                max_pnl_date = hist_pnl_m.idxmax().strftime("%Y-%m-%d")
                pnl_color = "#2EC4B6" if last_pnl >= 0 else "#E71D36"
                st.markdown(
                    f"<p style='color:#FFD700; font-size:16px; margin:0'>🏆 歷史最大損益：+NT$ {max_pnl_m:.2f}M　（{max_pnl_date}）</p>"
                    f"<p style='color:{pnl_color}; font-size:16px; margin:0'>目前損益：{sign}NT$ {last_pnl:.2f}M</p>",
                    unsafe_allow_html=True
                )
    except Exception as e:
        st.error(f"圖表載入失敗，請稍後重試。({e})")


def render_tab_us(us_live: dict, port: dict, grid: dict,
                  us_cash_usd: float, usd_twd: float, us_session: str = "",
                  cash_parking: list | None = None, grid_ticker: str = "", us_url: str = ""):
    grid_info = us_live.get(grid_ticker, {}) if grid_ticker else {}

    if grid_ticker and grid_info:
        grid_curr = grid_info.get("curr", 0)
        grid_yest = grid_info.get("yest", 0)
        grid_daily_pct = (grid_curr / grid_yest - 1) * 100 if grid_yest > 0 else 0

        st.caption(f"{grid_info.get('source','')} {us_session} {grid_info.get('time_str','')}")
        st.subheader(f"🎯 {grid_ticker} 網格進出戰略")

        g = grid
        cur_roi = (grid_curr / g["avg_price"] - 1) * 100 if g["avg_price"] > 0 else 0
        tp_dist = (g["tp_price"] / grid_curr - 1) * 100 if grid_curr > 0 and g["tp_price"] > 0 else 0
        add_dist = (g["next_add_price"] / grid_curr - 1) * 100 if grid_curr > 0 and g["next_add_price"] > 0 else 0
        est_profit = (g["tp_price"] - g["avg_price"]) * g["total_shares"] if g["avg_price"] > 0 else 0

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("目前進度", f"第 {g['tranche_no']} 份")
        c2.metric("目前股價", f"${grid_curr:.2f}", f"{grid_daily_pct:+.2f}%")
        c3.metric(f"平均股價 ({g['total_shares']:,.0f} 股)", f"${g['avg_price']:.2f}", f"{cur_roi:+.2f}%")
        c4.metric(f"目標停利 ({g['tp_pct']:.0f}%,  預估+${est_profit:,.0f})",
                  f"${g['tp_price']:.2f}", f"{tp_dist:+.2f}%" if grid_curr > 0 and g["tp_price"] > 0 else "N/A")
        if g["next_add_price"] > 0:
            c5.metric(f"加碼股價 ({g['next_add_shares']:,.0f} 股)", f"${g['next_add_price']:.2f}",
                      f"{add_dist:+.2f}%" if grid_curr > 0 else "N/A")
        else:
            c5.metric("加碼股價", "已滿倉", "無加碼空間")
        st.divider()
    else:
        st.info("ℹ️ 尚未設定「網格策略代號」或查無對應資料，已略過此區塊（屬選用功能）。")

    val  = port["val_us_usd"]
    cost = port["cost_us_usd"]
    us_roi = port["us_roi"]
    today_pnl = port["daily_pnl_usd"]
    yest_val  = sum(v["yest"] * v["shares"] for v in us_live.values())
    today_pct = today_pnl / yest_val if yest_val > 0 else 0

    valid_dates = [v["first_date"] for v in us_live.values() if pd.notnull(v.get("first_date"))]
    min_date_us = min(valid_dates) if valid_dates else pd.to_datetime("2024-01-01")
    days_us = max((datetime.today() - min_date_us).days, 1)
    ann_roi_us = ((1 + us_roi) ** (365 / days_us) - 1) * 100 if cost > 0 else 0

    u1, u2, u3, u4, u5 = st.columns(5)
    u1.metric("總市值 (USD)", f"${val:,.0f}")
    u2.metric("總投入成本",   f"${cost:,.0f}")
    u3.metric("未實現總損益", f"${val-cost:+,.0f}", f"{us_roi*100:+.2f}%")
    u4.metric("今日損益",     f"${today_pnl:+,.2f}", f"{today_pct*100:+.2f}%")
    u5.metric("曝險度",       f"{port['pct_us']:.1f}%")

    st.write("---")
    col_pie, col_info = st.columns([2, 1])
    with col_pie:
        st.write("📈 **美金資產配置比例 (USD)**")
        cd_total = sum(p["amount_usd"] for p in (cash_parking or []))
        labels = list(us_live.keys()) + ["美股可用現金", "CD/T-Bill停泊"]
        values = [v["curr"] * v["shares"] for v in us_live.values()] + [us_cash_usd, cd_total]
        if sum(values) > 0:
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.4,
                                         texttemplate="%{label}<br>$%{value:,.0f}<br>%{percent}")])
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("尚無資料可繪製。")
    with col_info:
        fc_us = port["fc_us_usd"]
        st.info(f"💡 **美股獨立淨資產**\n\nUS$ {fc_us:,.0f}\n\n*美股市值 + 美股現金 + CD/T-Bill停泊*")

    st.subheader("📦 個股明細")
    rows = []
    for t, info in us_live.items():
        avg = info["cost"] / info["shares"] if info["shares"] > 0 else 0
        l_roi = (info["curr"] / avg - 1) if avg > 0 else 0
        days_h = (datetime.today() - info["first_date"]).days if pd.notnull(info.get("first_date")) else 1
        l_ann  = ((1 + l_roi) ** (365 / max(days_h, 1)) - 1) * 100
        today_p = (info["curr"] - info["yest"]) * info["shares"]
        total_p = (info["curr"] - avg) * info["shares"]
        pct_d   = (info["curr"] / info["yest"] - 1) * 100 if info["yest"] > 0 else 0
        rows.append({
            "代號": t, "目前現價": f"${info['curr']:.2f}",
            "今日損益": f"${today_p:+,.2f} ({pct_d:+.2f}%)",
            "總損益":   f"${total_p:+,.2f} ({l_roi*100:+.2f}%)",
            "股數": f"{info['shares']:,.0f}", "均價": f"${avg:.2f}",
            "成本": f"${info['cost']:,.0f}", "昨日收盤": f"${info['yest']:.2f}",
            "年化報酬": f"{l_ann:+.2f}%",
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("尚無美股持倉資料（請確認「Trades」或「Leverage」分頁是否有正確填寫）。")

    st.write("---")
    st.subheader("🅿️ 資金停泊區")
    parking = cash_parking or []
    tmf_info = us_live.get("TMF", {})
    tmf_val  = tmf_info.get("curr", 0) * tmf_info.get("shares", 0)
    total_parked = sum(p["amount_usd"] for p in parking) + tmf_val

    if not parking and tmf_val == 0:
        st.info("目前無 CD / T-Bill 停泊紀錄。閒置資金建議停泊於短期美國國債，等待大跌機會。")
    else:
        st.caption(f"總閒置資金合計：**${total_parked:,.0f} USD**（含 TMF 市值）")
        if parking:
            park_rows = []
            for p in sorted(parking, key=lambda x: x["maturity"] or datetime.max.date()):
                days = p["days_left"]
                if days is None:
                    days_str, urgency = "N/A", ""
                elif days <= 0:
                    days_str, urgency = "✅ 已到期", "🔴"
                elif days <= 7:
                    days_str, urgency = f"⚠️ {days} 天後到期", "🟠"
                else:
                    days_str, urgency = f"{days} 天後到期", "🟡"
                park_rows.append({
                    "類型": p["type"], "金額 (USD)": f"${p['amount_usd']:,.0f}",
                    "到期日": str(p["maturity"]) if p["maturity"] else "N/A",
                    "狀態": f"{urgency} {days_str}", "備註": p["note"],
                })
            st.dataframe(pd.DataFrame(park_rows), use_container_width=True, hide_index=True)

    st.divider()
    if us_url:
        st.link_button("🛒 新增美股交易紀錄 (Google Sheets)", us_url, use_container_width=True)


def render_tab_lifecycle(port: dict, base_m: float, hc_years_default: int, target_k: float,
                         target_monthly_default: float, inflation_rate: float, withdrawal_rate: float,
                         usd_twd: float, target_asset_wan: float):
    st.subheader("⚖️ 生命周期曝險透視")

    val_tw  = port["val_tw_twd"]
    val_us  = port["val_us_usd"] * usd_twd
    total_p = val_tw + val_us
    tw_pct  = val_tw / total_p * 100 if total_p > 0 else 0
    us_pct  = val_us / total_p * 100 if total_p > 0 else 0

    c1, c2 = st.columns(2)
    c1.metric("💰 台股投資組合佔比", f"{tw_pct:.1f}%")
    c2.metric("💵 美股投資組合佔比", f"{us_pct:.1f}%")

    st.subheader("🎯 目標達成計算器")
    TARGET = target_asset_wan * 10_000

    fc_total_now = port["fc_total_twd"]
    gap = TARGET - fc_total_now
    gap_pct = (TARGET / fc_total_now - 1) * 100 if fc_total_now > 0 else 0

    col_t1, col_t2 = st.columns(2)
    col_t1.metric("目標資產", f"NT$ {TARGET/10000:,.0f} 萬")
    col_t2.metric("目前資產", f"NT$ {fc_total_now/10000:,.1f} 萬")

    if gap > 0:
        st.error(f"📉 距離目標還差 **NT$ {gap/10000:,.1f} 萬**（**{gap_pct:.1f}%**）")
    else:
        st.success(f"🎉 已超越目標！超出 NT$ {abs(gap)/10000:,.1f} 萬")

    rows_target = []
    for r in [2, 3, 4, 5, 6, 7]:
        if fc_total_now <= 0 or fc_total_now >= TARGET:
            n = 0
        else:
            n = math.ceil(math.log(TARGET / fc_total_now) / math.log(1 + r / 100))
        total_after = fc_total_now * ((1 + r / 100) ** n)
        rows_target.append({"每次漲幅": f"+{r}%", "需要幾次": f"{n} 次", "複利後資產 (萬)": f"{total_after/10000:,.1f}"})
    st.dataframe(pd.DataFrame(rows_target), use_container_width=True, hide_index=True)

    fc_tw    = port["fc_tw_twd"]
    fc_us    = port["fc_us_usd"] * usd_twd
    fc_total = port["fc_total_twd"]
    exp_tw   = port["exp_tw_twd"]
    exp_us   = port["exp_us_usd"] * usd_twd
    exp_tot  = port["exp_total_twd"]
    pct_tw   = port["pct_tw"]
    pct_us   = port["pct_us"]
    pct_tot  = port["pct_total"]

    st.markdown(rf"""
| 戰區 | 曝險金額  | 淨資產 | 獨立曝險度 |
| :--- | :--- | :--- | :--- |
| 💰 台股 | NT$ {exp_tw/10000:,.0f} 萬 | NT$ {fc_tw/10000:,.0f} 萬 | **{pct_tw:.1f}%** |
| 💵 美股 | NT\$ {exp_us/10000:,.0f} 萬<br/><span style="font-size: 0.85em; color: gray;"> {port['exp_us_usd']:,.0f} | NT\$ {fc_us/10000:,.0f} 萬<br/><span style="font-size: 0.85em; color: gray;">  {port['fc_us_usd']:,.0f} | **{pct_us:.1f}%** |
| 🔥 綜合 | **NT$ {exp_tot/10000:,.0f} 萬** | **NT$ {fc_total/10000:,.0f} 萬** | **{pct_tot:.1f}%** |
""", unsafe_allow_html=True)

    cur_hc_years   = st.session_state.get("lc_hc_years", hc_years_default)
    cur_target_wan = st.session_state.get("lc_target_wan", int(target_monthly_default // 10_000))
    prev_hc_years   = st.session_state.get("_prev_hc_years", cur_hc_years)
    prev_target_wan = st.session_state.get("_prev_target_wan", cur_target_wan)

    if cur_hc_years != prev_hc_years:
        st.session_state["lc_basis"] = "A"
    elif cur_target_wan != prev_target_wan:
        st.session_state["lc_basis"] = "B"
    basis = st.session_state.get("lc_basis", "A")

    st.session_state["_prev_hc_years"]   = cur_hc_years
    st.session_state["_prev_target_wan"] = cur_target_wan

    found_y, final_f, final_m = None, 0, 0
    tf = fc_total
    for y in range(1, 41):
        tf = tf * 1.08 + base_m * 12
        req = (cur_target_wan * 10_000) * ((1 + inflation_rate) ** y)
        if tf >= (req * 12) / withdrawal_rate:
            found_y, final_f, final_m = y, tf, req
            break

    effective_years = cur_hc_years if basis == "A" else (found_y or hc_years_default)
    note = "" if (basis != "B" or found_y) else "（40年內未達標，暫用預設年限）"
    st.caption(f"🎛️ 依「{'情境A' if basis == 'A' else '情境B'}」最近的調整，套用 {effective_years} 年{note}")

    W = fc_total + base_m * 12 * effective_years
    target_exp_val = W * (target_k / 100)
    target_exp_pct = (target_exp_val / fc_total * 100) if fc_total > 0 else 0

    ct, ca = st.columns(2)
    ct.metric("🎯 綜合目標曝險度", f"{target_exp_pct:.1f}%")
    ca.metric("🔥 綜合實際曝險度", f"{pct_tot:.1f}%", f"差距: {pct_tot - target_exp_pct:+.1f}%")

    st.subheader("⚖️ 應該如何平衡？")
    diff = exp_tot - target_exp_val
    if diff > 0:
        st.error(f"🚨 **目前總曝險過高！** 建議減少市場部位約 **NT$ {diff/10000:,.0f} 萬**")
        st.write(f"👉 **台股：** 若由台股調整，需減碼 NT$ {diff/2/10000:,.1f} 萬市值")
        st.write(f"👉 **美股：** 若由美股調整，需減碼 NT$ {diff/3/10000:,.1f} 萬市值")
    else:
        st.success(f"🟢 **目前曝險尚有空間！** 可增加市場部位約 **NT$ {abs(diff)/10000:,.0f} 萬**")

    st.divider()
    st.subheader("☕ 退休終局與提領反推")
    st.caption("＊通膨率、提領率等進階參數可在側邊欄「進階參數」中調整")

    st.markdown("**📈 情境 A：若工作幾年後退休？**")
    hc_years = st.number_input("工作年限（年）", min_value=1, max_value=40, value=hc_years_default, key="lc_hc_years")

    fa = fc_total
    for _ in range(hc_years):
        fa = fa * 1.08 + base_m * 12
    m_a     = fa * withdrawal_rate / 12
    m_a_now = m_a / ((1 + inflation_rate) ** hc_years)
    ca1, ca2, ca3 = st.columns(3)
    ca1.metric("屆時滾出資產",      f"NT$ {fa/10000:,.0f} 萬")
    ca2.metric("未來每月可領",       f"NT$ {m_a:,.0f}")
    ca3.metric("約等同現在每月可領", f"NT$ {m_a_now:,.0f}")

    st.write("")
    st.markdown("**🎯 情境 B：反推想月領幾萬的退休金？**")
    target_monthly_wan = st.number_input("目標月領（萬）", min_value=1, max_value=100,
                                         value=int(target_monthly_default // 10_000), key="lc_target_wan")
    if found_y:
        cb1, cb2, cb3 = st.columns(3)
        cb1.metric("需滾出資產",   f"NT$ {final_f/10000:,.0f} 萬")
        cb2.metric("未來每月可領", f"NT$ {final_m:,.0f}")
        cb3.metric("剩餘年限",     f"{found_y} 年")

    with st.expander("🛬 降落時程推演表 (Glide Path)", expanded=False):
        gp = []
        cf = fc_total
        for y in range(effective_years + 1):
            if y > 0:
                cf = cf * 1.08 + base_m * 12
            h_r = max(0, base_m * 12 * effective_years - base_m * 12 * y)
            e_g = ((cf + h_r) * target_k / 100) / cf * 100 if cf > 0 else 0
            gp.append({"年": f"第 {y} 年", "預估資產(萬)": f"{cf/10000:,.0f}", "應有曝險": f"{e_g:.1f}%"})
        st.table(pd.DataFrame(gp))


def render_tab_employee(price_info: dict, settings: dict):
    p_curr = price_info.get("curr", 0.0)
    p_yest = price_info.get("prev", 0.0)

    TOTAL_SHARES = settings["employee_shares"]
    COST_PRICE   = settings["employee_cost"]
    HEDGE_LOSS   = settings["employee_hedge_loss"]
    TOTAL_COST   = TOTAL_SHARES * COST_PRICE

    total_val  = TOTAL_SHARES * p_curr
    unrealized = total_val - TOTAL_COST
    net_profit = unrealized + HEDGE_LOSS
    daily_pnl  = (p_curr - p_yest) * TOTAL_SHARES
    daily_pct  = (p_curr / p_yest - 1) * 100 if p_yest > 0 else 0

    render_price_freshness(price_info.get("source", ""), price_info.get("time_str", ""),
                           price_info.get("age_min", 0), price_info.get("session", ""))

    st.subheader(f"🏭 {settings['employee_ticker']} 員工股／閉鎖股戰情")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("目前現貨價", f"{p_curr:.2f}")
    c2.metric("今日報酬", f"{daily_pnl:+,.0f} 元", f"{daily_pct:+.2f}%")
    c3.metric(f"總市值 ({TOTAL_SHARES/1000:,.1f} 張)", f"{total_val/10000:,.1f} 萬")
    c4.metric("未實現損益（現貨）", f"{unrealized/10000:+,.1f} 萬")
    c5.metric("避險損益（已實現）", f"{HEDGE_LOSS/10000:,.1f} 萬")
    c6.metric("實質總淨利", f"{net_profit/10000:+,.1f} 萬")

    st.divider()
    st.subheader("⏳ 解鎖時程與分批現值")
    now = datetime.today()
    rows = []
    for i, (d, ratio) in enumerate(settings["employee_tranches"], start=1):
        target_date = datetime.combine(d, datetime.min.time())
        days_left = max(0, (target_date - now).days)
        shares = TOTAL_SHARES * ratio
        val  = shares * p_curr
        cost = shares * COST_PRICE
        profit = val - cost
        rows.append({
            "解鎖梯次": f"第{i}梯次 ({ratio*100:.0f}%)",
            "預計日期": target_date.strftime("%Y/%m/%d"),
            "距離天數": f"約 {days_left} 天",
            "對應股數": f"{shares:,.0f} 股",
            "目前現值": f"{val:,.0f} 元",
            "未實現損益": f"{profit:+,.0f} 元",
        })
    if rows:
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("尚未設定解鎖梯次。")

    st.divider()
    st.subheader("🛡️ 避險與鎖利策略筆記")
    st.text_area("策略筆記（僅本次瀏覽，重新整理後不會保留）",
                 placeholder="可記錄目前避險狀態、停損點、下一步戰術評估…", height=120)

# ──────────────────────────────────────────
# ⑥ 側邊欄
# ──────────────────────────────────────────

def render_sidebar() -> dict:
    st.sidebar.header("🔗 我的試算表設定")
    st.sidebar.caption(
        "貼上你自己的 Google Sheet 連結（共用權限要設成「知道連結的人皆可檢視」）。"
        "沒有試算表嗎？先用範本檔案（台股帳本範本.xlsx／美股帳本範本.xlsx）匯入 Google Sheets 建立一份。"
    )
    tw_url = st.sidebar.text_input("📗 台股帳本連結", placeholder="https://docs.google.com/spreadsheets/d/...")
    tw_ticker = st.sidebar.text_input("台股追蹤代號", value=CONFIG.DEFAULT_TW_TICKER).strip().upper() or CONFIG.DEFAULT_TW_TICKER
    us_url = st.sidebar.text_input("📘 美股帳本連結", placeholder="https://docs.google.com/spreadsheets/d/...")

    with st.sidebar.expander("📐 股票分割還原設定（進階）", expanded=False):
        default_split = (tw_ticker == CONFIG.DEFAULT_TW_TICKER)
        split_enabled = st.checkbox("此標的曾經分割，需要價格還原", value=default_split)
        if split_enabled:
            split_cutoff_d = st.date_input("分割生效日", CONFIG.DEFAULT_SPLIT_CUTOFF.date())
            split_ratio    = st.number_input("分割比例（1股變幾股）", value=CONFIG.DEFAULT_SPLIT_RATIO, min_value=1.0)
            split_threshold = st.number_input("分割前股價門檻（高於此價才視為分割前舊價）", value=CONFIG.DEFAULT_SPLIT_THRESH)
        else:
            split_cutoff_d, split_ratio, split_threshold = CONFIG.DEFAULT_SPLIT_CUTOFF.date(), CONFIG.DEFAULT_SPLIT_RATIO, CONFIG.DEFAULT_SPLIT_THRESH

    st.sidebar.divider()

    with st.sidebar.expander("🏦 信貸試算（進階，預設關閉）", expanded=False):
        loan_enabled = st.checkbox("啟用信貸試算", value=False)
        loan1 = loan2 = 0.0
        if loan_enabled:
            l1_p = st.number_input("信貸一總額", value=0)
            l1_r = st.number_input("年利率1 (%)", value=2.5)
            l1_d = st.date_input("首次扣款日1", datetime.today())
            l1_y = st.number_input("貸款年限1", value=7, min_value=1)
            loan1, _ = calculate_loan(l1_p, l1_r, l1_y, l1_d)
            if l1_p > 0:
                st.info(f"貸1剩餘：{loan1/10000:.1f} 萬")
            st.divider()
            l2_p = st.number_input("信貸二總額", value=0)
            l2_r = st.number_input("年利率2 (%)", value=2.5)
            l2_d = st.date_input("首次扣款日2", datetime.today())
            l2_y = st.number_input("貸款年限2", value=7, min_value=1)
            loan2, _ = calculate_loan(l2_p, l2_r, l2_y, l2_d)
            if l2_p > 0:
                st.info(f"貸2剩餘：{loan2/10000:.1f} 萬")

    with st.sidebar.expander("🏭 員工股／閉鎖股設定（進階，預設關閉）", expanded=False):
        employee_enabled = st.checkbox("啟用員工股分頁", value=False)
        employee_ticker, employee_shares = "2408", 0.0
        employee_cost, employee_hedge_loss = 0.0, 0.0
        tranches = []
        if employee_enabled:
            employee_ticker = st.text_input("員工股代號", value="2408").strip()
            employee_shares = st.number_input("持股總數（股）", value=0)
            employee_cost   = st.number_input("每股成本", value=0.0)
            employee_hedge_loss = st.number_input("已實現避險損益（虧損請填負數）", value=0)
            tranche_count = st.number_input("解鎖梯次數", min_value=1, max_value=6, value=3)
            ratio_defaults = [50, 25, 25, 0, 0, 0]
            for i in range(int(tranche_count)):
                cc1, cc2 = st.columns(2)
                d = cc1.date_input(f"第{i+1}梯解鎖日", datetime(2027 + i, 4, 16), key=f"emp_d_{i}")
                r = cc2.number_input(f"第{i+1}梯比例(%)", value=ratio_defaults[i], key=f"emp_r_{i}")
                tranches.append((d, r / 100))

    with st.sidebar.expander("⚙️ 進階參數", expanded=False):
        usd_twd          = st.number_input("目前美元匯率", value=32.0)
        target_k         = st.number_input("一生目標曝險度 (%)", value=83)
        inflation_rate   = st.number_input("預估通膨 (%)", value=2.0) / 100
        withdrawal_rate  = st.number_input("安全提領率 (%)", value=4.0) / 100
        target_asset_wan = st.number_input("🎯 人生目標資產 (萬)", value=CONFIG.DEFAULT_TARGET_WAN, step=100.0)

    return dict(
        tw_url=tw_url.strip(), us_url=us_url.strip(), tw_ticker=tw_ticker,
        split_enabled=split_enabled, split_cutoff=pd.to_datetime(split_cutoff_d),
        split_ratio=split_ratio, split_threshold=split_threshold,
        loan_enabled=loan_enabled, loan1=loan1, loan2=loan2,
        employee_enabled=employee_enabled, employee_ticker=employee_ticker,
        employee_shares=employee_shares, employee_cost=employee_cost,
        employee_hedge_loss=employee_hedge_loss, employee_tranches=tranches,
        usd_twd=usd_twd, target_k=target_k, inflation_rate=inflation_rate,
        withdrawal_rate=withdrawal_rate, target_asset_wan=target_asset_wan,
        hc_years=11, target_monthly=100_000,
    )


# ──────────────────────────────────────────
# ⑦ 主程式（Main）
# ──────────────────────────────────────────

def main():
    st_autorefresh(interval=30000, key="war_room_refresh")

    st.markdown("""
        <style>
            .war-room-title { text-align: center; margin-top: -30px; margin-bottom: 20px; }
            .main-title { font-size: 100px !important; margin-bottom: 0px !important; font-weight: bold !important; line-height: 1.2 !important; }
            .sub-title { color: #888888 !important; font-size: 37px !important; letter-spacing: 5px !important; font-weight: 300 !important; margin-top: 10px !important; }
            .dash { display: inline !important; }
            @media (max-width: 768px) {
                .main-title { font-size: 42px !important; }
                .sub-title { font-size: 17px !important; letter-spacing: 2px !important; }
                .dash { display: none !important; }
            }
        </style>
        <div class="war-room-title">
            <h1 class="main-title">時間複利戰情室</h1>
            <p class="sub-title"><span class="dash">─────── </span>長線決策大腦 ⚔️ 絕對紀律執行<span class="dash"> ───────</span></p>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    if "analyzed" not in st.session_state:
        st.session_state.analyzed = False

    settings = render_sidebar()

    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
    except Exception as e:
        st.sidebar.error(f"連線初始化失敗: {e}")
        conn = None

    if not settings["tw_url"] and not settings["us_url"]:
        st.warning("📋 請先在左側「🔗 我的試算表設定」貼上至少一個試算表連結，才能開始分析。沒有試算表的話，可以先用範本檔案建立一份。")

    if st.button("🚀 啟動戰略掃描", use_container_width=True):
        st.session_state.analyzed = True
    if not st.session_state.analyzed:
        return

    fugle_key = st.secrets.get("FUGLE_API_KEY", "")
    if not fugle_key:
        st.sidebar.warning("⚠️ 未設定 FUGLE_API_KEY，台股報價將使用 yfinance")

    # ── 台股資料 ──
    df_tw_trades   = read_gsheets(conn, settings["tw_url"], worksheet="Trades") if conn else pd.DataFrame()
    df_tw_settings = read_gsheets(conn, settings["tw_url"], worksheet="Settings") if conn else pd.DataFrame()
    tw_kv = parse_kv_settings(df_tw_settings)
    base_m_wan = get_kv_float(tw_kv, "基準定額", default=CONFIG.DEFAULT_BASE_M_WAN)
    cash_wan   = get_kv_float(tw_kv, "現金部位", "現金", default=CONFIG.DEFAULT_CASH_WAN)
    base_m   = base_m_wan * 10_000
    cash_twd = cash_wan * 10_000

    tw_ticker = settings["tw_ticker"]
    tw_price = fetch_tw_price(tw_ticker, fugle_key=fugle_key)
    adj = make_split_adjuster(settings["split_enabled"], settings["split_cutoff"],
                              settings["split_ratio"], settings["split_threshold"])
    p_tw_curr = adj(tw_price["curr"])
    p_tw_yest = adj(tw_price["prev"])
    tw_trade = parse_tw_trades(df_tw_trades)

    # ── 美股資料 ──
    df_us_trades   = read_gsheets(conn, settings["us_url"], worksheet="Trades") if conn else pd.DataFrame()
    df_us_settings = read_gsheets(conn, settings["us_url"], worksheet="Settings") if conn else pd.DataFrame()
    df_us_etf      = read_gsheets(conn, settings["us_url"], worksheet="Leverage") if conn else pd.DataFrame()
    df_us_grid     = read_gsheets(conn, settings["us_url"], worksheet="Grid") if conn else pd.DataFrame()
    df_us_parking  = read_gsheets(conn, settings["us_url"], worksheet="Parking") if conn else pd.DataFrame()

    us_kv = parse_kv_settings(df_us_settings)
    us_cash_usd = get_kv_float(us_kv, "可用現金", "現金", default=0.0)
    grid_ticker = get_kv_str(us_kv, "網格策略代號", "網格").upper()

    leverage_overrides = parse_etf_leverage(df_us_etf)
    tickers_from_trades = []
    if not df_us_trades.empty:
        col_t = find_col(df_us_trades, "代號", "代碼")
        if col_t:
            tickers_from_trades = sorted({str(x).strip().upper() for x in df_us_trades[col_t].dropna() if str(x).strip()})
    all_tickers = sorted(set(list(leverage_overrides.keys()) + tickers_from_trades + ([grid_ticker] if grid_ticker else [])))

    us_live, us_session = {}, ""
    for t in all_tickers:
        trade = parse_us_trades(df_us_trades, t)
        price = fetch_us_price(t)
        if not us_session:
            us_session = price.get("session", "")
        us_live[t] = {**trade, "curr": price["curr"], "yest": price["prev"],
                      "session": price["session"], "source": price["source"], "time_str": price["time_str"]}

    leverage_lookup = {t: leverage_overrides.get(t, CONFIG.DEFAULT_LEVERAGE_MAP.get(t, 1)) for t in all_tickers}

    grid = parse_soxl_grid(df_us_grid) if grid_ticker else dict(
        tranche_no=0, total_shares=0, avg_price=0, tp_price=0, tp_pct=0, next_add_price=0, next_add_shares=0)
    cash_parking = parse_cash_parking(df_us_parking)

    loan1 = settings["loan1"] if settings["loan_enabled"] else 0.0
    loan2 = settings["loan2"] if settings["loan_enabled"] else 0.0
    loan_total = loan1 + loan2

    port = compute_portfolio(
        tw_trade, us_live, p_tw_curr, p_tw_yest, cash_twd, loan_total,
        us_cash_usd, settings["usd_twd"], cash_parking=cash_parking, leverage_map=leverage_lookup,
    )

    tab_labels = ["💰 台股", "💵 美股", "🛬 生命周期 & 退休"]
    if settings["employee_enabled"]:
        tab_labels.append("🏭 員工股")
    tabs = st.tabs(tab_labels)

    with tabs[0]:
        if not settings["tw_url"]:
            render_onboarding_tw()
        else:
            render_tab_tw(tw_trade, port, p_tw_curr, p_tw_yest, base_m, loan1, loan2, cash_twd,
                         tw_price=tw_price, tw_ticker=tw_ticker,
                         loan_enabled=settings["loan_enabled"], tw_url=settings["tw_url"])

    with tabs[1]:
        if not settings["us_url"]:
            render_onboarding_us()
        else:
            render_tab_us(us_live, port, grid, us_cash_usd, settings["usd_twd"], us_session,
                         cash_parking, grid_ticker=grid_ticker, us_url=settings["us_url"])

    with tabs[2]:
        render_tab_lifecycle(
            port, base_m, settings["hc_years"], settings["target_k"], settings["target_monthly"],
            settings["inflation_rate"], settings["withdrawal_rate"], settings["usd_twd"],
            settings["target_asset_wan"],
        )

    if settings["employee_enabled"]:
        with tabs[3]:
            if settings["employee_ticker"]:
                emp_price = fetch_tw_price(settings["employee_ticker"], fugle_key=fugle_key)
            else:
                emp_price = dict(curr=0.0, prev=0.0, source="N/A", time_str="N/A", age_min=99999, session="")
            render_tab_employee(emp_price, settings)

    st.caption("📱 V12.0 多人共用版 ｜ 貼上自己的試算表連結即可使用 ｜ 資料 / 計算 / UI 三層分離")


if __name__ == "__main__" or True:
    main()
