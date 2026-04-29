import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection
from datetime import datetime, timedelta
import calendar
import pytz
from streamlit_autorefresh import st_autorefresh

# ==========================================
# 時間複利戰情室 V11.0 - 模組化整理版
# 長線決策大腦 ╳ 絕對紀律執行 (開發者：賴賴)
# ==========================================

# ① 設定分頁標題
st.set_page_config(page_title="時間複利戰情室 | 賴賴", page_icon="💰", layout="wide")

# ──────────────────────────────────────────
# ① 全域常數（CONFIG）
# ──────────────────────────────────────────
class CONFIG:
    TITLE          = "⚔️ 賴賴戰情室 V11.2"
    TICKER_TW      = "00631L"
    TICKER_TW_YF   = "00631L.TW"
    SPLIT_CUTOFF   = pd.to_datetime("2026-03-23")
    SPLIT_RATIO    = 22.0
    SPLIT_THRESH   = 100.0          # 分割前股價門檻

    US_TICKERS     = ["SOXL", "TMF", "BITX"]
    # 各 ETF「名目槓桿倍數」用於曝險計算
    # TMF = 0：長債持倉不計入股市曝險（與舊版行為一致）
    LEVERAGE_MAP   = {"SOXL": 3, "BITX": 2, "TMF": 0}

    SHEET_TW = "https://docs.google.com/spreadsheets/d/1yYs-JIW4-8jr8EoyyWlydNrE5Gtd_frWdlMQVdn1VYk/edit?usp=sharing"
    SHEET_US = "https://docs.google.com/spreadsheets/d/1-NPhyuRNWSarFPdgHjUkB9J3smSbn3u3fjUbMhMVyfI/edit?usp=sharing"

    PRICE_TTL      = 60             # 報價 cache 秒數
    DEFAULT_BASE_M_WAN = 10.0       # 基準定額預設(萬)
    DEFAULT_CASH_WAN   = 200.0      # 台幣現金預設(萬)

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


def apply_split_adj(price: float, date: pd.Timestamp) -> float:
    """將分割前的價格還原為分割後等效價格（統一入口）"""
    if date < CONFIG.SPLIT_CUTOFF and price > CONFIG.SPLIT_THRESH:
        return round(price / CONFIG.SPLIT_RATIO, 2)
    return round(price, 2)


def apply_split_adj_shares(shares: float, price: float, date: pd.Timestamp) -> float:
    """將分割前的股數還原為分割後等效股數"""
    if date < CONFIG.SPLIT_CUTOFF and price > CONFIG.SPLIT_THRESH:
        return shares * CONFIG.SPLIT_RATIO
    return shares


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
    wd = now.weekday()  # 0=Mon, 6=Sun
    if wd >= 5:
        return "🌙 週末休市"
    pre_open  = dt_mod.time(8, 0)
    open_t    = dt_mod.time(9, 0)
    close_t   = dt_mod.time(13, 30)
    after_t   = dt_mod.time(14, 30)   # 盤後定價交易結束
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
    """
    回傳 dict: curr, prev, source, time_str, age_min
    優先 Fugle → yfinance fast_info → yfinance history
    注意：fugle_key 從外部傳入，避免在 cache 函式內讀 st.secrets（會不穩定）
    """
    # --- Fugle ---
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
        except Exception as e:
            pass  # fallback，錯誤訊息由呼叫端顯示

    yf_sym = ticker + ".TW"

    # --- yfinance fast_info ---
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

    # --- yfinance history fallback ---
    try:
        hist = yf.download(yf_sym, period="5d", progress=False)
        closes = (hist["Close"][yf_sym] if isinstance(hist.columns, pd.MultiIndex)
                  else hist["Close"]).dropna()
        curr = float(closes.iloc[-1])
        prev = float(closes.iloc[-2]) if len(closes) >= 2 else curr
        return dict(curr=curr, prev=prev, source="🔴 yfinance 歷史備援",
                    time_str="歷史資料", age_min=9999, session=get_tw_session_label())
    except Exception:
        return dict(curr=1.0, prev=1.0, source="❌ 完全失敗", time_str="N/A", age_min=99999, session="❓")


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
    """根據美東時間判斷目前交易時段標籤"""
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
    import pytz, datetime as dt_mod
    et_tz = pytz.timezone("America/New_York")
    now_et = dt_mod.datetime.now(et_tz)
    session = _get_us_session_label(now_et)

    # ── yfinance 盤前盤後精準抓取 (主引擎) ──
    try:
        tkr = yf.Ticker(ticker)
        # 強制開啟 prepost=True，抓取包含盤前盤後的最近 2 天 1 分鐘 K 線
        hist = tkr.history(period="2d", interval="1m", prepost=True)
        
        if not hist.empty:
            curr = float(hist["Close"].iloc[-1])
            # 昨收使用 fast_info 取得會比較精準穩定
            prev = float(tkr.fast_info.previous_close)
            
            # 取得這筆報價的精準時間
            last_time = hist.index[-1].astimezone(et_tz)
            time_str = last_time.strftime("%Y-%m-%d %H:%M ET")
            
            return dict(curr=curr, prev=prev, session=session,
                        source="🟢 yfinance ", time_str=time_str)
    except Exception as e:
        pass

    # ── 完全失敗的最後防線 ──
    return dict(curr=0.0, prev=0.0, session="❓",
                source="❌ 完全失敗", time_str="N/A")


def read_gsheets(conn, url: str, **kwargs) -> pd.DataFrame:
    """安全讀取 Google Sheets，失敗回空 DataFrame 並顯示警告"""
    try:
        df = conn.read(spreadsheet=url, ttl=0, **kwargs)
        return df if df is not None else pd.DataFrame()
    except Exception as e:
        st.sidebar.error(f"❌ Google Sheets 讀取失敗: {e}")
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
    """解析台股交易紀錄，回傳彙總資訊"""
    result = dict(shares=0.0, cost=0.0, min_date=pd.NaT, raw_buys=pd.DataFrame())
    if df_raw.empty or "交易類型" not in df_raw.columns:
        return result
    df = df_raw.copy()
    df["成交日期"] = pd.to_datetime(df["成交日期"])
    df["庫存股數"]  = pd.to_numeric(df["庫存股數"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["持有成本"]  = pd.to_numeric(df["持有成本"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    is_sell = df["交易類型"].str.contains("賣出", na=False)
    df.loc[is_sell, ["庫存股數", "持有成本"]] *= -1
    result["shares"]   = df["庫存股數"].sum()
    result["cost"]     = df["持有成本"].sum()
    result["min_date"] = df["成交日期"].min()
    # ⚠️ raw_buys 必須保留原始所有欄（含「成交價格」），
    # 因為 render_tab_tw 裡的逐筆戰績表需要存取 r["成交價格"]
    raw_buy_mask = df_raw["交易類型"].str.contains("買入", na=False)
    result["raw_buys"] = df_raw[raw_buy_mask].copy()
    if not result["raw_buys"].empty:
        result["raw_buys"]["成交日期"] = pd.to_datetime(result["raw_buys"]["成交日期"])
    return result


def parse_us_trades(df_raw: pd.DataFrame, ticker: str) -> dict:
    """解析單一美股代號的交易紀錄"""
    result = dict(shares=0.0, cost=0.0, first_date=pd.NaT)
    if df_raw.empty or "股票代號" not in df_raw.columns:
        return result
    df = df_raw[df_raw["股票代號"] == ticker].copy()
    if df.empty:
        return result
    df["成交日期"] = pd.to_datetime(df["成交日期"])
    df["庫存股數"]  = pd.to_numeric(df["庫存股數"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df["持有成本"]  = pd.to_numeric(df["持有成本"].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    is_sell = df["交易類型"].str.contains("賣出", na=False)
    df.loc[is_sell, ["庫存股數", "持有成本"]] *= -1
    result["shares"]     = df["庫存股數"].sum()
    result["cost"]       = df["持有成本"].sum()
    result["first_date"] = df["成交日期"].min()
    return result


def parse_soxl_grid(df_raw: pd.DataFrame) -> dict:
    """
    從美股帳本 DataFrame 中解析 SOXL 網格規則。
    回傳 dict:
      tranche_no, total_shares, avg_price, tp_price, tp_pct,
      next_add_price, next_add_shares
    """
    empty = dict(tranche_no=0, total_shares=0, avg_price=0, tp_price=0,
                 tp_pct=0, next_add_price=0, next_add_shares=0)
    if df_raw.empty:
        return empty

    # 動態找欄位（標題含關鍵字即可）
    def find_col(keyword):
        return next((c for c in df_raw.columns if keyword in str(c)), None)

    col_k = find_col("實際股數")
    col_l = find_col("實際成本價")
    col_m = find_col("實際停利股價")
    col_d = find_col("預估股價")
    col_e = find_col("預估股數")
    col_g = find_col("停利%")

    if col_k is None:
        return empty

    df = df_raw.copy()
    df["_K"] = pd.to_numeric(df[col_k].astype(str).str.replace(r"[^\d.]", "", regex=True), errors="coerce").fillna(0)

    # 只保留「預估股價 > 0」的有效列（去除空白與雜訊）
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
        # 下一階
        next_idx = active_df.index[-1] + 1
        if next_idx < len(valid_df):
            nr = valid_df.iloc[next_idx]
            result["next_add_price"]  = to_float(nr[col_d]) if col_d else 0
            result["next_add_shares"] = to_float(nr[col_e]) if col_e else 0
    else:
        # 全空倉：第一列當加碼目標
        if not valid_df.empty:
            fr = valid_df.iloc[0]
            result["next_add_price"]  = to_float(fr[col_d]) if col_d else 0
            result["next_add_shares"] = to_float(fr[col_e]) if col_e else 0
    return result


def parse_cash_parking(df_raw: pd.DataFrame) -> list[dict]:
    """
    解析美股帳本中的「資金停泊區」（CD / T-Bill）。
    Google Sheets 格式（獨立區塊，標題列含關鍵字）：
      停泊類型 | 金額(USD) | 到期日 | 備註
    回傳 list of dict，每筆含：type, amount_usd, maturity, note, days_left
    """
    result = []
    if df_raw.empty:
        return result

    col_type = next((c for c in df_raw.columns if "停泊" in str(c) or "類型" in str(c) and "停" in str(c)), None)
    col_amt  = next((c for c in df_raw.columns if "停泊" in str(c) and "金額" in str(c) or ("金額" in str(c))), None)
    col_mat  = next((c for c in df_raw.columns if "到期" in str(c)), None)
    col_note = next((c for c in df_raw.columns if "備註" in str(c) and col_type and c != col_type), None)

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
        mat_date = None
        days_left = None
        try:
            mat_date  = pd.to_datetime(mat_raw).date()
            days_left = (mat_date - today).days
        except Exception:
            pass
        note = str(row.get(col_note, "")).strip() if col_note else ""
        result.append(dict(
            type=t, amount_usd=amt,
            maturity=mat_date, days_left=days_left, note=note
        ))
    return result


def compute_portfolio(tw_trade: dict, us_live: dict,
                      p_tw_curr: float, p_tw_yest: float,
                      cash_twd: float, loan_twd: float,
                      us_cash_usd: float, usd_twd: float,
                      cash_parking: list = None) -> dict:
    """
    彙整雙帳戶資產、曝險度。
    所有台幣金額後綴 _twd，美元後綴 _usd。
    """
    # --- 台股 ---
    val_tw_twd  = tw_trade["shares"] * p_tw_curr
    cost_tw_twd = tw_trade["cost"]
    exp_tw_twd  = val_tw_twd * 2
    fc_tw_twd   = val_tw_twd + cash_twd - loan_twd
    pct_tw      = (exp_tw_twd / fc_tw_twd * 100) if fc_tw_twd > 0 else 0
    daily_pnl_twd = (p_tw_curr - p_tw_yest) * tw_trade["shares"]
    roi_tw      = (val_tw_twd / cost_tw_twd - 1) if cost_tw_twd > 0 else 0

    # --- 美股 ---
    val_us_usd  = sum(v["shares"] * v["curr"] for v in us_live.values())
    cost_us_usd = sum(v["cost"]   for v in us_live.values())
    exp_us_usd  = sum(
        v["shares"] * v["curr"] * CONFIG.LEVERAGE_MAP.get(t, 1)
        for t, v in us_live.items()
    )
    cd_total_usd = sum(p["amount_usd"] for p in (cash_parking or []))
    fc_us_usd   = val_us_usd + us_cash_usd + cd_total_usd
    pct_us      = (exp_us_usd / fc_us_usd * 100) if fc_us_usd > 0 else 0
    daily_pnl_usd = sum((v["curr"] - v["yest"]) * v["shares"] for v in us_live.values())
    us_roi      = (val_us_usd / cost_us_usd - 1) if cost_us_usd > 0 else 0

    # --- 綜合（統一換算台幣）---
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
    """顯示報價新鮮度 + 交易時段 caption"""
    session_tag = f" {session}" if session else ""
    if age_min < 60:
        st.caption(f"{source}{session_tag} {time_str}（{age_min:.0f} 分鐘前）")
    elif age_min < 480:
        st.caption(f"{source}{session_tag} {time_str}（{age_min/60:.1f} 小時前）")
    elif age_min < 9999:
        st.caption(f"{source}{session_tag} {time_str}（{age_min/60:.1f} 小時前）")
    else:
        st.caption(f"{source}{session_tag} ｜ 使用歷史收盤價（{time_str}）")


def render_tab_tw(tw_trade: dict, port: dict, p_tw_curr: float, p_tw_yest: float,
                  base_m: float, loan1: float, loan2: float, cash_twd: float,
                  tw_price: dict = None):
    """Tab 1 台股完整 UI"""
    # 報價來源 caption（放在 Tab1 內部頂端）
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

    # --- 基本指標列 ---
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

    # --- 雙引擎戰略 ---
    st.subheader("🚨 雙引擎戰略")
    roi_pct = roi * 100

    # 動態基準
    if roi_pct >= 0:
        adj_pct = min(roi_pct, 20.0)
        dynamic_m = max(base_m * (1 - adj_pct/100), base_m * 0.8)
        adj_str = f"降 {adj_pct:.1f}% (獲利調節)"
    else:
        adj_pct = min(abs(roi_pct) * 2, 100.0)
        dynamic_m = min(base_m * (1 + adj_pct/100), base_m * 2.0)
        adj_str = f"升 {adj_pct:.1f}% (虧損加碼)"

    # 回款日
    today_d  = datetime.today().date()
    dca_date = next_first_wednesday(today_d)
    is_dca   = (today_d == dca_date)

    # 狙擊
    sniper_mult, sniper_label = sniper_signal(daily_pct)
    sniper_m = dynamic_m * sniper_mult

    # 最終行動
    if is_dca and sniper_m > 0:
        final_amt    = max(dynamic_m, sniper_m)
        action_label = "🔥 定額與狙擊撞日 (擇高投入)"
    elif is_dca:
        final_amt    = dynamic_m
        action_label = "📅 執行每月動態定額"
    elif sniper_m > 0:
        final_amt    = sniper_m
        action_label = f"🎯 執行階梯狙擊 ({sniper_label})"
    else:
        final_amt    = 0
        action_label = "觀望不動"

    st.info("💡 **資金鐵則：** 帳戶請隨時鎖定 6 倍現金流，作為戰略預備金。")
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

    # --- 瀑布圖 ---
    col_p, col_d = st.columns([2, 1])
    with col_p:
        st.write("📊 **台幣資產與淨值變動 (瀑布圖)**")
        loan_total = -(loan1 + loan2)
        net = val + cash_twd + loan_total
        fig = go.Figure(go.Waterfall(
            orientation="v",
            x=["00631L 市值", "可用現金", "信貸總餘額", "台股獨立淨資產"],
            measure=["relative", "relative", "relative", "total"],
            y=[val, cash_twd, loan_total, net],
            textposition="inside",
            texttemplate="NT$ %{y:,.0f}",
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
        st.info(f"💡 **台股獨立淨資產**\n\nNT$ {port['fc_tw_twd']/10000:,.1f} 萬\n\n*台股市值 + 台幣現金 − 總信貸*")

    # --- 逐筆戰績 ---
    with st.expander(f"📜 逐筆投資戰績表 (目前現價: {p_tw_curr:.2f})", expanded=False):
        buy_df = tw_trade.get("raw_buys", pd.DataFrame())
        if not buy_df.empty:
            recs = []
            for _, r in buy_df.sort_values("成交日期", ascending=False).iterrows():
                # 帳本已全面使用分割後尺度，直接讀取，不需要 split 換算
                adj_p = to_float(r.get("成交價格", 0))
                adj_s = to_float(r.get("庫存股數", 0))
                l_pnl = adj_s * p_tw_curr - r["持有成本"]
                l_roi = l_pnl / r["持有成本"] if r["持有成本"] > 0 else 0
                days_held = max((datetime.today() - r["成交日期"]).days, 1)
                l_ann = ((1 + l_roi) ** (365 / days_held) - 1) * 100
                recs.append({
                    "日期": r["成交日期"].strftime("%Y-%m-%d"),
                    "買價": f"{adj_p:.2f}",
                    "股數": f"{adj_s:,.0f}",
                    "目前現價": f"{p_tw_curr:.2f}",
                    "今日損益": f"{(p_tw_curr - p_tw_yest) * adj_s:+,.0f}",
                    "總損益": f"{l_pnl:+,.0f}",
                    "年化報酬": f"{l_ann:+.1f}%",
                    "總報酬": f"{l_roi*100:+.1f}%",
                })
            st.dataframe(pd.DataFrame(recs), use_container_width=True, hide_index=True)

    # --- 圖表分析 ---
    st.subheader("🌐 戰術圖表分析")
    _render_tw_charts(tw_trade, p_tw_curr, p_tw_yest)

    st.divider()
    st.link_button("🛒 新增台股交易紀錄 (Google Sheets)", CONFIG.SHEET_TW, use_container_width=True)


def _render_tw_charts(tw_trade: dict, p_tw_curr: float, p_tw_yest: float):
    """台股三張戰術圖表"""
    try:
        hist = yf.download(CONFIG.TICKER_TW_YF, period="5y", progress=False)
        raw_close = (hist["Close"][CONFIG.TICKER_TW_YF]
                     if isinstance(hist.columns, pd.MultiIndex) else hist["Close"])

        # yfinance 的 Close 欄已是 adjusted price（股票分割前的舊價格會自動縮小還原），
        # 不需要再手動除以 SPLIT_RATIO，直接使用即可。
        adj = raw_close.copy()

        min_date = tw_trade["min_date"]
        start    = min_date if pd.notnull(min_date) else pd.to_datetime("2024-01-01")
        rp       = adj[adj.index >= start]

        if rp.dropna().empty:
            return

        # 帳本已全面使用分割後尺度（低股價 + 多股數），yfinance adjusted close 也是同一尺度，直接相除即可。
        avg_cost = tw_trade["cost"] / tw_trade["shares"] if tw_trade["shares"] > 0 else 0

        # A. 價格走勢
        st.write("📈 **A. 價格走勢與還原均價**")
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=rp.index, y=rp.values, name="還原價", line=dict(color="#E71D36")))
        mx, mi, lt = rp.max(), rp.min(), rp.dropna().iloc[-1]
        if avg_cost > 0:
            fig1.add_hline(y=avg_cost, line_dash="dash", line_color="#00A86B",
                           annotation_text=f"🟢 均價線: {avg_cost:.2f}")
            fig1.add_hrect(y0=avg_cost, y1=max(mx*1.1, avg_cost*1.1),
                           fillcolor="green", opacity=0.1, layer="below", line_width=0)
            fig1.add_hrect(y0=min(mi*0.9, avg_cost*0.9), y1=avg_cost,
                           fillcolor="red", opacity=0.1, layer="below", line_width=0)
        fig1.add_annotation(x=rp.idxmax(), y=mx, text=f"高:{mx:.2f}", showarrow=True, ay=-30)
        fig1.add_annotation(x=rp.idxmin(), y=mi, text=f"低:{mi:.2f}", showarrow=True, ay=30)
        fig1.add_annotation(x=rp.index[-1], y=lt, text=f"最新:{lt:.2f}", showarrow=True, ax=40)
        y0 = min(mi*0.9, avg_cost*0.9) if avg_cost > 0 else mi*0.9
        y1 = max(mx*1.1, avg_cost*1.1) if avg_cost > 0 else mx*1.1
        fig1.update_yaxes(range=[y0, y1])
        st.plotly_chart(fig1, use_container_width=True)

        # B. 乖離率
        st.write("📊 **B. 多空戰術乖離率**")
        bias = (rp - rp.rolling(20).mean()) / rp.rolling(20).mean() * 100
        bc   = bias.dropna()
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

        # C. 損益軌跡
        st.write("💰 **C. 庫存真實損益軌跡**")
        buy_df = tw_trade.get("raw_buys", pd.DataFrame())
        if not buy_df.empty:
            th = buy_df.groupby("成交日期")[["庫存股數", "持有成本"]].sum().reindex(rp.index).fillna(0)
            # 帳本已是分割後多股數，與 adjusted price 同尺度，直接 cumsum 即可。
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
        # D. 成本 vs 市值（金額軌跡）
        st.write("💴 **D. 庫存成本 vs 市值 金額軌跡**")
        if not buy_df.empty:
            mv_m = (ds * rp) / 1_000_000
            cc_m = dc.reindex(rp.index).ffill() / 1_000_000
            mv_m = mv_m.dropna()
            cc_m = cc_m.reindex(mv_m.index)

            last_pnl = mv_m.iloc[-1] - cc_m.iloc[-1]
            sign = "+" if last_pnl >= 0 else ""

            fig4 = go.Figure()
            fig4.add_trace(go.Scatter(x=cc_m.index, y=cc_m.values, name="累積成本", line=dict(color="#888888", width=2)))
            fig4.add_trace(go.Scatter(x=mv_m.index, y=mv_m.values, name="市值", line=dict(color="#2EC4B6", width=2.5)))
            fig4.add_annotation(x=mv_m.idxmax(), y=mv_m.max(), text=f"最高:{mv_m.max():.2f}M", showarrow=True, ay=-30)
            fig4.add_annotation(x=mv_m.index[-1], y=mv_m.iloc[-1], text=f"最新:{mv_m.iloc[-1]:.2f}M", showarrow=True, ax=40)
            fig4.add_annotation(x=cc_m.index[-1], y=cc_m.iloc[-1], text=f"成本:{cc_m.iloc[-1]:.2f}M", showarrow=True, ay=30, ax=40)
            st.plotly_chart(fig4, use_container_width=True)

            # 損益單獨一行顯示在圖下方
            pnl_color = "#2EC4B6" if last_pnl >= 0 else "#E71D36"
            st.markdown(f"<p style='color:{pnl_color}; font-size:16px; margin:0'>目前損益：{sign}NT$ {last_pnl:.2f}M</p>", unsafe_allow_html=True)
    except Exception as e:
        st.error(f"圖表載入失敗，請稍後重試。({e})")


def render_tab_us(us_live: dict, port: dict, grid: dict,
                  us_cash_usd: float, usd_twd: float, us_session: str = "",
                  cash_parking: list = None):
    """Tab 2 美股完整 UI"""
    soxl = us_live.get("SOXL", {})
    soxl_curr = soxl.get("curr", 0)
    soxl_yest = soxl.get("yest", 0)
    soxl_daily_pct = (soxl_curr / soxl_yest - 1) * 100 if soxl_yest > 0 else 0

    # 報價來源與時段
    source_info = soxl.get("source", "")
    time_info   = soxl.get("time_str", "")
    st.caption(f"{source_info} {us_session} {time_info}")

    st.subheader("🎯 SOXL 網格進出戰略")

    # 網格指標
    g = grid
    cur_roi = (soxl_curr / g["avg_price"] - 1) * 100 if g["avg_price"] > 0 else 0
    tp_dist = (g["tp_price"] / soxl_curr - 1) * 100 if soxl_curr > 0 and g["tp_price"] > 0 else 0
    add_dist= (g["next_add_price"] / soxl_curr - 1) * 100 if soxl_curr > 0 and g["next_add_price"] > 0 else 0
    est_profit = (g["tp_price"] - g["avg_price"]) * g["total_shares"] if g["avg_price"] > 0 else 0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("目前進度",  f"第 {g['tranche_no']} 份")
    c2.metric("目前股價",  f"${soxl_curr:.2f}", f"今日 {soxl_daily_pct:+.2f}%")
    c3.metric(f"平均股價 ({g['total_shares']:,.0f} 股)", f"${g['avg_price']:.2f}", f"{cur_roi:+.2f}%")
    c4.metric(f"目標停利 ({g['tp_pct']:.0f}%,  預估+${est_profit:,.0f})",
              f"${g['tp_price']:.2f}", f"差距 {tp_dist:+.2f}%" if soxl_curr > 0 and g["tp_price"] > 0 else "N/A")
    if g["next_add_price"] > 0:
        c5.metric(f"加碼股價 ({g['next_add_shares']:,.0f} 股)",
                  f"${g['next_add_price']:.2f}", f"差距 {add_dist:+.2f}%" if soxl_curr > 0 else "N/A")
    else:
        c5.metric("加碼股價", "已滿倉", "無加碼空間")

    st.divider()

    # 整體美股指標
    val  = port["val_us_usd"]
    cost = port["cost_us_usd"]
    us_roi = port["us_roi"]
    today_pnl = port["daily_pnl_usd"]
    yest_val  = sum(v["yest"] * v["shares"] for v in us_live.values())
    today_pct = today_pnl / yest_val if yest_val > 0 else 0

    valid_dates = [v["first_date"] for v in us_live.values() if pd.notnull(v.get("first_date"))]
    min_date_us = min(valid_dates) if valid_dates else pd.to_datetime("2024-01-01")
    days_us = max((datetime.today() - min_date_us).days, 1)
    ann_roi_us = ((1 + us_roi) ** (365 / days_us) - 1) * 100

    u1, u2, u3, u4, u5 = st.columns(5)
    u1.metric("總市值 (USD)",  f"${val:,.0f}")
    u2.metric("總投入成本",    f"${cost:,.0f}")
    u3.metric("未實現總損益",  f"${val-cost:+,.0f}", f"{us_roi*100:+.2f}%")
    u4.metric("今日損益",      f"${today_pnl:+,.2f}", f"{today_pct*100:+.2f}%")
    u5.metric("曝險度",        f"{port['pct_us']:.1f}%")

    # 圓餅圖 + 淨資產
    st.write("---")
    col_pie, col_info = st.columns([2, 1])
    with col_pie:
        st.write("📈 **美金資產配置比例 (USD)**")
        # 加入 CD 停泊資金
        cd_total = sum(p["amount_usd"] for p in (cash_parking or []))
        labels = list(us_live.keys()) + ["美股可用現金", "CD停泊"]
        values = [v["curr"] * v["shares"] for v in us_live.values()] + [us_cash_usd, cd_total]
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.4,
                                     texttemplate="%{label}<br>$%{value:,.0f}<br>%{percent}")])
        fig.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    with col_info:
        fc_us = port["fc_us_usd"]
        st.info(f"💡 **美股獨立淨資產**\n\nUS$ {fc_us:,.0f}\n\n*美股市值 + 美股現金 + CD停泊*")

    # 個股明細
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
        session_label = info.get("session", "")
        rows.append({
            "代號": t,
            "目前現價": f"${info['curr']:.2f}",
            "今日損益": f"${today_p:+,.2f} ({pct_d:+.2f}%)",
            "總損益":   f"${total_p:+,.2f} ({l_roi*100:+.2f}%)",
            "股數": f"{info['shares']:,.0f}",
            "股數": f"{info['shares']:,.0f}",
            "均價": f"${avg:.2f}",
            "成本": f"${info['cost']:,.0f}",
            "昨日收盤": f"${info['yest']:.2f}",
            "年化報酬": f"{l_ann:+.2f}%",
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    st.write("---")

    # ── 資金停泊區 UI (移動至此，移除 expander 改為直接展開) ──
    st.subheader("🅿️ 資金停泊區")
    parking  = cash_parking or []
    tmf_info = us_live.get("TMF", {})
    tmf_val  = tmf_info.get("curr", 0) * tmf_info.get("shares", 0)
    total_parked = sum(p["amount_usd"] for p in parking) + tmf_val

    if not parking and tmf_val == 0:
        st.info("目前無 CD / T-Bill 停泊紀錄。閒置資金建議停泊於 **1～3 個月期美國國債**，等待大跌機會。")
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
                    "類型": p["type"],
                    "金額 (USD)": f"${p['amount_usd']:,.0f}",
                    "到期日": str(p["maturity"]) if p["maturity"] else "N/A",
                    "狀態": f"{urgency} {days_str}",
                    "備註": p["note"],
                })
            st.dataframe(pd.DataFrame(park_rows), use_container_width=True, hide_index=True)
        if tmf_val > 0:
            tmf_shares = tmf_info.get("shares", 0)
            tmf_price  = tmf_info.get("curr", 0)
            

    st.divider()
    st.link_button("🛒 新增美股交易紀錄 (Google Sheets)", CONFIG.SHEET_US, use_container_width=True)


def render_tab_lifecycle(port: dict, base_m: float, hc_years_default: int, target_k: float,
                         target_monthly_default: float, inflation_rate: float, withdrawal_rate: float,
                         usd_twd: float):
    """Tab 3 生命周期 & 退休"""
    st.subheader("⚖️ 生命周期曝險透視")

    val_tw  = port["val_tw_twd"]
    val_us  = port["val_us_usd"] * usd_twd
    total_p = val_tw + val_us
    tw_pct  = val_tw / total_p * 100 if total_p > 0 else 0
    us_pct  = val_us / total_p * 100 if total_p > 0 else 0

    c1, c2 = st.columns(2)
    c1.metric("💰 台股投資組合佔比", f"{tw_pct:.1f}%")
    c2.metric("💵 美股投資組合佔比", f"{us_pct:.1f}%")

    fc_tw    = port["fc_tw_twd"]
    fc_us    = port["fc_us_usd"] * usd_twd
    fc_total = port["fc_total_twd"]
    exp_tw   = port["exp_tw_twd"]
    exp_us   = port["exp_us_usd"] * usd_twd
    exp_tot  = port["exp_total_twd"]
    pct_tw   = port["pct_tw"]
    pct_us   = port["pct_us"]
    pct_tot  = port["pct_total"]

    st.markdown(f"""
| 戰區 | 曝險金額  | 淨資產 | 獨立曝險度 |
| :--- | :--- | :--- | :--- |
| 💰 台股 | NT$ {exp_tw/10000:,.0f} 萬 | NT$ {fc_tw/10000:,.0f} 萬 | **{pct_tw:.1f}%** |
| 💵 美股 | NT\$ {exp_us/10000:,.0f} 萬<br/><span style="font-size: 0.85em; color: gray;"> {port['exp_us_usd']:,.0f} | NT\$ {fc_us/10000:,.0f} 萬<br/><span style="font-size: 0.85em; color: gray;">  {port['fc_us_usd']:,.0f} | **{pct_us:.1f}%** |
| 🔥 綜合 | **NT$ {exp_tot/10000:,.0f} 萬** | **NT$ {fc_total/10000:,.0f} 萬** | **{pct_tot:.1f}%** |
""", unsafe_allow_html=True)

    # 目標曝險度
    W = fc_total + base_m * 12 * hc_years_default
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

    # 退休試算
    st.divider()
    st.subheader("☕ 退休終局與提領反推")
    st.caption("＊通膨率、提領率等進階參數可在側邊欄「進階參數」中調整（預設：通膨 2%、提領率 4%）")

        # ── 情境 A ──
    st.markdown("**📈 情境 A：若工作幾年後退休？**")
    hc_years = st.number_input("工作年限（年）", min_value=1, max_value=40,
                                value=hc_years_default)

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

    # ── 情境 B ──
    st.markdown("**🎯 情境 B：反推想月領幾萬的退休金？**")
    target_monthly_wan = st.number_input("目標月領（萬）", min_value=1, max_value=100,
                                         value=int(target_monthly_default // 10_000))
    target_monthly_now = target_monthly_wan * 10_000

    found_y, final_f, final_m = None, 0, 0
    tf = fc_total
    for y in range(1, 41):
        tf = tf * 1.08 + base_m * 12
        req = target_monthly_now * ((1 + inflation_rate) ** y)
        if tf >= (req * 12) / withdrawal_rate:
            found_y, final_f, final_m = y, tf, req
            break
    if found_y:
        cb1, cb2, cb3 = st.columns(3)
        cb1.metric("需滾出資產",   f"NT$ {final_f/10000:,.0f} 萬")
        cb2.metric("未來每月可領", f"NT$ {final_m:,.0f}")
        cb3.metric("剩餘年限",     f"{found_y} 年")

    # 降落時程表
    with st.expander("🛬 降落時程推演表 (Glide Path)", expanded=False):
        gp = []
        cf = fc_total
        for y in range(hc_years + 1):
            if y > 0:
                cf = cf * 1.08 + base_m * 12
            h_r = max(0, base_m * 12 * hc_years - base_m * 12 * y)
            e_g = ((cf + h_r) * target_k / 100) / cf * 100 if cf > 0 else 0
            gp.append({"年": f"第 {y} 年", "預估資產(萬)": f"{cf/10000:,.0f}", "應有曝險": f"{e_g:.1f}%"})
        st.table(pd.DataFrame(gp))


def render_tab_nanya(price_info: dict):
    """Tab 4 南亞科專屬頁面"""
    p_curr = price_info.get("curr", 0.0)
    p_yest = price_info.get("prev", 0.0)
    
    # 員工股固定參數設定
    TOTAL_SHARES = 32_000         # 總數 32 張
    COST_PRICE   = 32.75           # 每股成本
    HEDGE_LOSS   = -260_000       # 歷史避險虧損 (已實現，負值)
    
    TOTAL_COST = TOTAL_SHARES * COST_PRICE
    
    # 即時計算
    total_val  = TOTAL_SHARES * p_curr
    unrealized = total_val - TOTAL_COST
    net_profit = unrealized + HEDGE_LOSS   # 實質淨利 = 未實現 + 已實現虧損
    daily_pnl  = (p_curr - p_yest) * TOTAL_SHARES
    daily_pct  = (p_curr / p_yest - 1) * 100 if p_yest > 0 else 0
    
    render_price_freshness(price_info.get("source", ""), price_info.get("time_str", ""), 
                           price_info.get("age_min", 0), price_info.get("session", ""))
    
    st.subheader("🏭 南亞科 (2408) 員工股鎖利與避險戰情")
    
    
    # ── 頂部指標 ──
    # 新增今日報酬，將欄位改為 6 欄顯示
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("目前現貨價", f"{p_curr:.2f}")
    c2.metric("今日報酬", f"{daily_pnl:+,.0f} 元", f"{daily_pct:+.2f}%")
    c3.metric("總市值 (32 張)", f"{total_val/10000:,.1f} 萬")
    c4.metric("未實現損益 (現貨)", f"{unrealized/10000:+,.1f} 萬")
    c5.metric("避險虧損 (已實現)", f"{HEDGE_LOSS/10000:,.1f} 萬")
    c6.metric("實質總淨利", f"{net_profit/10000:+,.1f} 萬")
    
    st.divider()
    
    # ── 解鎖時程與分批現值 ──
    st.subheader("⏳ 解鎖時程與分批現值")
    
    now = datetime.today()
    
    def calc_tranche(date_str: str, share_ratio: float):
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
        days_left   = (target_date - now).days
        shares      = TOTAL_SHARES * share_ratio
        val         = shares * p_curr
        cost        = shares * COST_PRICE
        profit      = val - cost
        return target_date.strftime("%Y/%m"), max(0, days_left), shares, val, profit
        
    t1_month, t1_days, t1_s, t1_v, t1_p = calc_tranche("2027-04-16", 0.50)
    t2_month, t2_days, t2_s, t2_v, t2_p = calc_tranche("2028-04-16", 0.25)
    t3_month, t3_days, t3_s, t3_v, t3_p = calc_tranche("2029-04-16", 0.25)

    # 用 DataFrame 來呈現解鎖表格會更整齊
    df_schedule = pd.DataFrame([
        {"解鎖梯次": "第一梯次 (50%)", "預計月份": t1_month, "距離天數": f"約 {t1_days} 天", 
         "對應張數": f"{t1_s/1000:.0f} 張", "目前現值": f"{t1_v:,.0f} 元", "未實現損益": f"{t1_p:+,.0f} 元"},
         
        {"解鎖梯次": "第二梯次 (25%)", "預計月份": t2_month, "距離天數": f"約 {t2_days} 天", 
         "對應張數": f"{t2_s/1000:.0f} 張", "目前現值": f"{t2_v:,.0f} 元", "未實現損益": f"{t2_p:+,.0f} 元"},
         
        {"解鎖梯次": "第三梯次 (25%)", "預計月份": t3_month, "距離天數": f"約 {t3_days} 天", 
         "對應張數": f"{t3_s/1000:.0f} 張", "目前現值": f"{t3_v:,.0f} 元", "未實現損益": f"{t3_p:+,.0f} 元"}
    ])
    st.dataframe(df_schedule, use_container_width=True, hide_index=True)
    
    st.divider()
    
    # ── 避險與鎖利策略區塊 ──
    st.subheader("🛡️ 避險與鎖利策略")
    
    st.info("💡 **待執行：** 距離解鎖還有一段時間，不知何時需要放空鎖定價差，需持續尋找合適時機點。")
    
    col_history, col_action = st.columns(2)
    with col_history:
        st.markdown("""
        **【歷史紀律與教訓】**
        * 庫存成本：**32.75 元**
        * 曾經放空：**105 元** (南亞科期貨)
        * 認賠平倉：**255 元** (虧損 **26 萬**)
        * **總結**：提早避險卻遇上暴漲，導致虧損。未來執行避險時，需嚴設停損或改用選擇權控制最大風險。
        """)
        
    with col_action:
        st.markdown("""
        **【目前防護狀態】**
        * 狀態：🔴 **無任何避險部位** (完全裸險)
        * 風險暴露：目前所有市值完全承受現貨價格漲跌風險。
        * **下一步戰術評估**：
            1. 觀察季線/年線乖離率，若出現極端超買再考慮重新放空。
            2. 越接近解鎖日 (2027/04/16)，鎖定價格的急迫性越高。
        """)

# ──────────────────────────────────────────
# ⑥ 側邊欄
# ──────────────────────────────────────────

def render_sidebar() -> dict:
    """側邊欄設定，回傳所有參數 dict"""
    st.sidebar.header("🏦 資金與貸款設定")

    with st.sidebar.expander("🏦 貸款細項設定", expanded=False):
        l1_p = st.number_input("信貸一總額",   value=2_830_000)
        l1_r = st.number_input("年利率1 (%)",  value=2.28)
        l1_d = st.date_input("首次扣款日1",    datetime(2024, 1, 15))
        loan1, pmt1 = calculate_loan(l1_p, l1_r, 7, l1_d)
        st.info(f"貸1剩餘：{loan1/10000:.1f} 萬")
        st.divider()
        l2_p = st.number_input("信貸二總額",   value=950_000)
        l2_r = st.number_input("年利率2 (%)",  value=2.72)
        l2_d = st.date_input("首次扣款日2",    datetime(2026, 3, 5))
        loan2, pmt2 = calculate_loan(l2_p, l2_r, 10, l2_d)
        st.info(f"貸2剩餘：{loan2/10000:.1f} 萬")

    st.sidebar.divider()

    with st.sidebar.expander("⚙️ 進階參數（通常不需調整）", expanded=False):
        usd_twd         = st.number_input("4. 目前美元匯率",       value=32.0)
        target_k        = st.number_input("6. 一生目標曝險度 (%)", value=83)
        inflation_rate  = st.number_input("8. 預估通膨 (%)",       value=2.0) / 100
        withdrawal_rate = st.number_input("9. 安全提領率 (%)",     value=4.0) / 100

    # 5 和 7 改在 Tab3 內填寫，這裡給預設值讓 main() 傳入
    return dict(
        loan1=loan1, loan2=loan2, pmt1=pmt1, pmt2=pmt2,
        usd_twd=usd_twd, target_k=target_k,
        inflation_rate=inflation_rate,
        withdrawal_rate=withdrawal_rate,
        # hc_years 和 target_monthly 已移至 Tab3，這裡給佔位預設值
        hc_years=11,
        target_monthly=100_000,
    )

    st.sidebar.divider()
    st.sidebar.header("⚙️ 生命周期與退休規劃")
    usd_twd           = st.sidebar.number_input("4. 目前美元匯率",         value=32.0)
    hc_years          = st.sidebar.number_input("5. 預計剩餘投入年限",       value=11)
    target_k          = st.sidebar.number_input("6. 一生目標曝險度 (%)",     value=83)
    target_monthly    = st.sidebar.number_input("7. 目標月領金額 (現值)",    value=100_000, step=10_000)
    inflation_rate    = st.sidebar.number_input("8. 預估通膨 (%)",           value=2.0) / 100
    withdrawal_rate   = st.sidebar.number_input("9. 安全提領率 (%)",         value=4.0) / 100

    return dict(
        loan1=loan1, loan2=loan2, pmt1=pmt1, pmt2=pmt2,
        usd_twd=usd_twd, hc_years=int(hc_years), target_k=target_k,
        target_monthly=target_monthly, inflation_rate=inflation_rate,
        withdrawal_rate=withdrawal_rate,
    )


# ──────────────────────────────────────────
# ⑦ 主程式（Main）
# ──────────────────────────────────────────

def main():
    # 👇 在 main() 的最開頭加上這行 👇
    # interval=60000 代表 60000 毫秒 (即 60 秒)
    # key="war_room_refresh" 是給這個計時器一個專屬的內部標籤，避免衝突
    st_autorefresh(interval=30000, key="war_room_refresh")
    # 這裡取代原本的 st.title(CONFIG.TITLE)
    # ② 顯示戰情室大標題 (強制放大版)
    st.markdown("""
        <style>
            /* 預設（電腦版）的樣式：加上 !important 強制覆蓋 Streamlit 預設設定 */
            .war-room-title { text-align: center; margin-top: -30px; margin-bottom: 20px; }
            .main-title { font-size: 100px !important; margin-bottom: 0px !important; font-weight: bold !important; line-height: 1.2 !important; } 
            .sub-title { color: #888888 !important; font-size: 37px !important; letter-spacing: 5px !important; font-weight: 300 !important; margin-top: 10px !important; }
            .dash { display: inline !important; }

            /* 當螢幕寬度小於 768px（手機版）時觸發以下樣式 */
            @media (max-width: 768px) {
                .main-title { font-size: 42px !important; } 
                .sub-title { font-size: 17px !important; letter-spacing: 2px !important; } 
                .dash { display: none !important; }
            }
        </style>

        <div class="war-room-title">
            <h1 class="main-title">時間複利戰情室</h1>
            <p class="sub-title">
                <span class="dash">─────── </span>長線決策大腦 ⚔️ 絕對紀律執行<span class="dash"> ───────</span>
            </p>
        </div>
    """, unsafe_allow_html=True)
    st.divider()

    if "analyzed" not in st.session_state:
        st.session_state.analyzed = False

    params = render_sidebar()

    # --- Google Sheets 連線 ---
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
    except Exception as e:
        st.sidebar.error(f"連線初始化失敗: {e}")
        conn = None

    # --- 讀取台股帳本 ---
    base_m_wan = CONFIG.DEFAULT_BASE_M_WAN
    cash_wan   = CONFIG.DEFAULT_CASH_WAN
    df_tw_raw  = pd.DataFrame()

    if conn:
        df_tw_raw = read_gsheets(conn, CONFIG.SHEET_TW)
        if not df_tw_raw.empty:
            st.sidebar.success("✅ 台股帳本同步成功！")
            try:
                while len(df_tw_raw.columns) < 11:
                    df_tw_raw[f"_pad_{len(df_tw_raw.columns)}"] = np.nan
                vj = to_float(df_tw_raw.iloc[0, 9])
                vk = to_float(df_tw_raw.iloc[0, 10])
                if vj: base_m_wan = vj
                if vk: cash_wan   = vk
            except Exception as e:
                st.sidebar.warning(f"⚠️ 參數欄位解析失敗，用預設值。({e})")
            st.sidebar.info(f"🏦 自動載入台股參數：\n基準定額 **{base_m_wan:,.0f} 萬** | 現金 **{cash_wan:,.0f} 萬**")

    base_m    = base_m_wan * 10_000
    cash_twd  = cash_wan   * 10_000

    # --- 讀取美股帳本 ---
    us_cash_usd = 0.0
    df_us_raw   = pd.DataFrame()

    if conn:
        df_raw_no_hdr = read_gsheets(conn, CONFIG.SHEET_US, header=None)
        if not df_raw_no_hdr.empty:
            st.sidebar.success("✅ 美股資料庫同步成功！")
            df_us_raw = df_raw_no_hdr.copy()
            df_us_raw.columns = df_us_raw.iloc[0]
            df_us_raw = df_us_raw[1:].reset_index(drop=True)
            # 讀取 I7 可用現金
            try:
                if len(df_raw_no_hdr) >= 7 and len(df_raw_no_hdr.columns) >= 9:
                    us_cash_usd = to_float(df_raw_no_hdr.iloc[6, 8])
                st.sidebar.info(f"💵 美股可用現金 (I7): ${us_cash_usd:,.2f}")
            except Exception as e:
                st.sidebar.warning(f"⚠️ 無法解析 I7 現金欄位: {e}")

    # --- 啟動按鈕 ---
    if st.button("🚀 啟動戰略掃描", use_container_width=True):
        st.session_state.analyzed = True

    if not st.session_state.analyzed:
        return

    # ── API Keys（在 cache 函式外讀取，避免 cache 內 st.secrets 不穩定）──
    fugle_key = st.secrets.get("FUGLE_API_KEY", "")

    if not fugle_key:
        st.sidebar.warning("⚠️ 未設定 FUGLE_API_KEY，台股報價將使用 yfinance")

    # ── 資料擷取 ──
    tw_price = fetch_tw_price(CONFIG.TICKER_TW, fugle_key=fugle_key)
    
    # 新增：獲取南亞科(2408)即時報價
    nanya_price = fetch_tw_price("2408", fugle_key=fugle_key)

    # 台股 split 還原：
    #   分割後（2026-03-23 起）Fugle/yfinance 回傳的已是低價（≤ SPLIT_THRESH），直接用。
    #   若極罕見地仍收到舊格式高價（> SPLIT_THRESH），則除以 SPLIT_RATIO 還原。
    def _adj_live_price(p: float) -> float:
        return round(p / CONFIG.SPLIT_RATIO, 2) if p > CONFIG.SPLIT_THRESH else round(p, 2)

    p_tw_curr = _adj_live_price(tw_price["curr"])
    p_tw_yest = _adj_live_price(tw_price["prev"])

    # 解析台股交易
    tw_trade = parse_tw_trades(df_tw_raw)

    # 解析美股交易 + 即時報價
    us_live = {}
    us_session = ""
    import pytz, datetime as dt_mod
    et_tz = pytz.timezone("America/New_York")
    us_session = _get_us_session_label(dt_mod.datetime.now(et_tz))

    for t in CONFIG.US_TICKERS:
        trade = parse_us_trades(df_us_raw, t)
        price = fetch_us_price(t)
        if not us_session:
            us_session = price.get("session", "")
        us_live[t] = {
            **trade,
            "curr":     price["curr"],
            "yest":     price["prev"],
            "session":  price["session"],
            "source":   price["source"],
            "time_str": price["time_str"],
        }

    # 解析 SOXL 網格
    grid = parse_soxl_grid(df_us_raw)

    # 解析資金停泊區（CD / T-Bill）
    cash_parking = parse_cash_parking(df_us_raw)

    # 計算資產組合
    loan_total = params["loan1"] + params["loan2"]
    port = compute_portfolio(
    tw_trade, us_live,
    p_tw_curr, p_tw_yest,
    cash_twd, loan_total,
    us_cash_usd, params["usd_twd"],
    cash_parking=cash_parking,
    )

    # ── 渲染四個 Tab ──
    tab1, tab2, tab3, tab4 = st.tabs(["💰 台股", "💵 美股", "🛬 生命周期 & 退休", "🏭 南亞科"])

    with tab1:
        render_tab_tw(tw_trade, port, p_tw_curr, p_tw_yest,
                      base_m, params["loan1"], params["loan2"], cash_twd,
                      tw_price=tw_price)

    with tab2:
        render_tab_us(us_live, port, grid, us_cash_usd, params["usd_twd"], us_session, cash_parking)

    with tab3:
        render_tab_lifecycle(
            port, base_m,
            params["hc_years"], params["target_k"],
            params["target_monthly"], params["inflation_rate"],
            params["withdrawal_rate"], params["usd_twd"],
        )
        
    with tab4:
        render_tab_nanya(nanya_price)

    st.caption("📱 V11.2 模組化整理版 | 新增南亞科獨立戰情頁面 | 資料 / 計算 / UI 三層分離")


if __name__ == "__main__" or True:
    main()
