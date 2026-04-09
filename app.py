import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from streamlit_gsheets import GSheetsConnection
from datetime import datetime, timedelta

# ==========================================
# 賴賴投資戰情室 V8.8 - 架構重構版
# ==========================================

st.set_page_config(page_title="賴賴終極戰情室", page_icon="💰", layout="wide")
st.title("🛡️ 賴賴投資戰情室 V8.8")

if "analyzed" not in st.session_state:
    st.session_state.analyzed = False

TICKER_TW = "00631L.TW"
SPLIT_CUTOFF = pd.to_datetime('2026-03-23')   # 分割基準日，2026/3/23 之前的資料需還原
SPLIT_RATIO = 22.0
SHEET_TW = "https://docs.google.com/spreadsheets/d/1yYs-JIW4-8jr8EoyyWlydNrE5Gtd_frWdlMQVdn1VYk/edit?usp=sharing"
SHEET_US = "https://docs.google.com/spreadsheets/d/1-NPhyuRNWSarFPdgHjUkB9J3smSbn3u3fjUbMhMVyfI/edit?usp=sharing"

# ==========================================
# 🔧 核心計算函數區
# ==========================================

def calculate_loan_remaining(principal, annual_rate, years, start_date):
    """計算貸款剩餘本金與每月還款額"""
    if principal <= 0 or years <= 0:
        return 0, 0
    r = annual_rate / 100 / 12
    N = years * 12
    pmt = principal * r * (1 + r) ** N / ((1 + r) ** N - 1) if r > 0 else principal / N
    today = datetime.today().date()
    passed_months = (today.year - start_date.year) * 12 + (today.month - start_date.month)
    if today.day >= start_date.day:
        passed_months += 1
    passed_months = max(0, min(passed_months, int(N)))
    rem_balance = (
        principal * ((1 + r) ** N - (1 + r) ** passed_months) / ((1 + r) ** N - 1)
        if r > 0
        else principal - (pmt * passed_months)
    )
    return max(0, rem_balance), pmt


def adjust_price_for_split(price, date):
    """
    修正：用日期判斷是否需要做分割還原，避免用價格 > 100 誤判。
    2026/3/23 之前的收盤價 / 22，之後的直接使用。
    """
    if pd.to_datetime(date) < SPLIT_CUTOFF:
        return price / SPLIT_RATIO
    return price


def adjust_shares_for_split(shares, date):
    """2026/3/23 之前成交的股數 * 22，還原為現在的股數單位"""
    if pd.to_datetime(date) < SPLIT_CUTOFF:
        return shares * SPLIT_RATIO
    return shares


# ==========================================
# 📡 資料抓取函數（含 cache）
# ==========================================

@st.cache_data(ttl=300)
def fetch_tw_history():
    """台股歷史資料，5 分鐘快取"""
    return yf.download(TICKER_TW, period="max", progress=False)["Close"]


@st.cache_data(ttl=300)
def fetch_us_history():
    """美股歷史資料，5 分鐘快取"""
    tickers = ["SOXX", "SOXL", "TMF", "BITX"]
    return yf.download(tickers, period="200d", progress=False)


@st.cache_data(ttl=60)
def fetch_tw_live():
    """台股即時價（1 分鐘快取）"""
    tkr = yf.Ticker(TICKER_TW)
    try:
        raw_curr = float(tkr.fast_info.last_price)
        raw_prev = float(tkr.fast_info.previous_close)
    except Exception:
        hist = yf.download(TICKER_TW, period="2d", progress=False)
        raw_curr = float(hist["Close"].iloc[-1])
        raw_prev = float(hist["Close"].iloc[-2])
    # 用今天日期判斷是否需要還原（若今天已過分割日則不還原）
    today = pd.Timestamp.today().normalize()
    curr = adjust_price_for_split(raw_curr, today)
    prev = adjust_price_for_split(raw_prev, today)
    return round(curr, 2), round(prev, 2)


# ==========================================
# 🧮 台股計算函數
# ==========================================

def calc_tw_portfolio(df_tw_raw):
    """從帳本計算台股持倉彙總，回傳 dict"""
    if df_tw_raw.empty:
        return {"shares": 0, "cost": 0, "min_date": pd.to_datetime("2024-01-01"), "df": pd.DataFrame()}

    temp = df_tw_raw.copy()
    temp["成交日期"] = pd.to_datetime(temp["成交日期"])
    # 賣出視為負數
    temp.loc[temp["交易類型"].str.contains("賣出", na=False), ["庫存股數", "持有成本"]] *= -1
    return {
        "shares": temp["庫存股數"].sum(),
        "cost": temp["持有成本"].sum(),
        "min_date": temp["成交日期"].min(),
        "df": temp,
    }


def calc_tw_metrics(tw_port, p_curr, p_prev):
    """計算台股損益指標"""
    shares = tw_port["shares"]
    cost = tw_port["cost"]
    cur_val = shares * p_curr
    roi = (cur_val / cost - 1) if cost > 0 else 0
    days = max((datetime.today() - tw_port["min_date"]).days, 1) if pd.notnull(tw_port["min_date"]) else 1
    ann_roi = ((1 + roi) ** (365 / days) - 1) * 100
    today_pnl = (p_curr - p_prev) * shares
    today_pct = (p_curr / p_prev - 1) * 100 if p_prev > 0 else 0
    avg_cost = cost / shares if shares > 0 else 0
    return {
        "cur_val": cur_val,
        "roi": roi,
        "ann_roi": ann_roi,
        "today_pnl": today_pnl,
        "today_pct": today_pct,
        "avg_cost": avg_cost,
    }


# ==========================================
# 🧮 美股計算函數
# ==========================================

def calc_us_portfolio(df_us_raw, us_data):
    """從帳本計算美股各標的持倉，回傳 dict"""
    us_live = {}
    total_val_usd = 0.0
    total_cost_usd = 0.0

    for t in ["SOXL", "TMF", "BITX"]:
        if not df_us_raw.empty and "股票代號" in df_us_raw.columns:
            t_data = df_us_raw[df_us_raw["股票代號"] == t].copy()
        else:
            t_data = pd.DataFrame()

        if not t_data.empty:
            t_data["成交日期"] = pd.to_datetime(t_data["成交日期"])
            t_data.loc[t_data["交易類型"].str.contains("賣出", na=False), ["庫存股數", "持有成本"]] *= -1
            shares = t_data["庫存股數"].sum()
            cost = t_data["持有成本"].sum()
            first_d = t_data["成交日期"].min()
        else:
            shares, cost, first_d = 0, 0, pd.NaT

        curr_p = float(us_data["Close"][t].dropna().iloc[-1])
        yest_p = float(us_data["Close"][t].dropna().iloc[-2])
        us_live[t] = {
            "shares": shares,
            "cost": cost,
            "curr": curr_p,
            "yest": yest_p,
            "first_date": first_d,
        }
        total_val_usd += shares * curr_p
        total_cost_usd += cost

    return us_live, total_val_usd, total_cost_usd


def calc_us_metrics(us_live, total_val_usd, total_cost_usd):
    """計算美股整體損益指標"""
    valid_dates = [v["first_date"] for v in us_live.values() if pd.notnull(v["first_date"])]
    min_date_us = min(valid_dates) if valid_dates else pd.to_datetime("2024-01-01")
    us_roi = (total_val_usd / total_cost_usd - 1) if total_cost_usd > 0 else 0
    ann_roi_us = ((1 + us_roi) ** (365 / max((datetime.today() - min_date_us).days, 1)) - 1) * 100
    total_today_pnl = sum([(v["curr"] - v["yest"]) * v["shares"] for v in us_live.values()])
    total_yest_val = sum([v["yest"] * v["shares"] for v in us_live.values()])
    today_pct = (total_today_pnl / total_yest_val) if total_yest_val > 0 else 0
    # 實際曝險 = SOXL x3 + BITX x2
    exp_usd = us_live["SOXL"]["curr"] * us_live["SOXL"]["shares"] * 3 + us_live["BITX"]["curr"] * us_live["BITX"]["shares"] * 2
    return {
        "us_roi": us_roi,
        "ann_roi_us": ann_roi_us,
        "today_pnl": total_today_pnl,
        "today_pct": today_pct,
        "exp_usd": exp_usd,
    }


# ==========================================
# 🧮 淨資產 & 曝險計算
# ==========================================

def calc_fc_and_exposure(cur_val_tw, total_us_val_usd, us_cash_usd, cash, loan1, loan2, usd_twd):
    """計算淨資產 FC 與各區曝險度"""
    total_us_val_twd = (total_us_val_usd + us_cash_usd) * usd_twd
    FC = cur_val_tw + total_us_val_twd + cash - (loan1 + loan2)
    FC = max(FC, 1)  # 防止除以零
    return FC, total_us_val_twd


# ==========================================
# 側邊欄：全局參數
# ==========================================

st.sidebar.header("⚙️ 資金與曝險參數")
base_m_wan = st.sidebar.number_input("1. 基準每月定期定額 (萬)", value=10.0, step=1.0)
cash_wan = st.sidebar.number_input("2. 目前帳戶可用現金 (萬)", value=200.0, step=10.0)
us_cash_usd = st.sidebar.number_input("3. 美股可用現金 (USD)", value=235.73, step=100.0)
target_exp_pct = st.sidebar.number_input("4. 設定目標曝險度 (%)", value=200)

base_m = base_m_wan * 10000
cash = cash_wan * 10000

with st.sidebar.expander("🏦 貸款細項設定 (自動連動)", expanded=False):
    l1_p = st.number_input("信貸一總額", value=2830000)
    l1_r = st.number_input("年利率1(%)", value=2.28)
    l1_d = st.date_input("首次扣款日1", datetime(2024, 1, 15))
    loan1, pmt1 = calculate_loan_remaining(l1_p, l1_r, 7, l1_d)
    st.info(f"貸1剩餘：{loan1/10000:.1f}萬")
    st.divider()
    l2_p = st.number_input("信貸二總額", value=950000)
    l2_r = st.number_input("年利率2(%)", value=2.72)
    l2_d = st.date_input("首次扣款日2", datetime(2026, 3, 5))
    loan2, pmt2 = calculate_loan_remaining(l2_p, l2_r, 10, l2_d)
    st.info(f"貸2剩餘：{loan2/10000:.1f}萬")

st.sidebar.divider()
st.sidebar.header("⚙️ 生命周期與退休規劃")
usd_twd = st.sidebar.number_input("6. 目前美元匯率", value=32.0)
hc_years = st.sidebar.number_input("7. 預計剩餘投入年限", value=11)
target_k = st.sidebar.number_input("8. 一生目標曝險度 (%)", value=83)
target_monthly_now = st.sidebar.number_input("9. 目標月領金額 (現值)", value=100000, step=10000)
inflation_rate = st.sidebar.number_input("10. 預估通膨 (%)", value=2.0) / 100.0
withdrawal_rate = st.sidebar.number_input("11. 安全提領率 (%)", value=4.0) / 100.0

# ==========================================
# 🚀 雲端帳本同步
# ==========================================

try:
    conn = st.connection("gsheets", type=GSheetsConnection)
    df_tw_raw = conn.read(spreadsheet=SHEET_TW, ttl=0)
    df_us_raw = conn.read(spreadsheet=SHEET_US, ttl=0)
    st.sidebar.success("✅ 台美股雙帳本同步成功！")
except Exception as e:
    st.sidebar.error(f"❌ 帳本連結失敗：{e}")
    df_tw_raw = pd.DataFrame()
    df_us_raw = pd.DataFrame()

col_btn, col_refresh = st.columns([4, 1])
with col_btn:
    if st.button("🚀 啟動戰略掃描", use_container_width=True):
        st.session_state.analyzed = True
with col_refresh:
    if st.button("🔄 清除快取", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# ==========================================
# 🎯 主要分析區塊
# ==========================================

if st.session_state.analyzed:

    # --- 資料抓取 ---
    p_tw_curr, p_tw_yest = fetch_tw_live()
    hist_tw_raw = fetch_tw_history()
    us_data = fetch_us_history()

    # --- 台股還原歷史價格（用日期，不用價格判斷）---
    if isinstance(hist_tw_raw, pd.DataFrame):
        hist_tw_raw = hist_tw_raw.iloc[:, 0]
    adj_hist_tw = hist_tw_raw.copy()
    adj_hist_tw.loc[adj_hist_tw.index < SPLIT_CUTOFF] /= SPLIT_RATIO

    # --- 台股投資組合 ---
    tw_port = calc_tw_portfolio(df_tw_raw)
    tw_m = calc_tw_metrics(tw_port, p_tw_curr, p_tw_yest)

    # --- 美股投資組合 ---
    us_live, total_us_val_usd, total_us_cost_usd = calc_us_portfolio(df_us_raw, us_data)
    us_m = calc_us_metrics(us_live, total_us_val_usd, total_us_cost_usd)

    # --- 淨資產 & 曝險（提前算好，避免 walrus operator 藏在 metric 裡）---
    FC, total_us_val_twd = calc_fc_and_exposure(
        tw_m["cur_val"], total_us_val_usd, us_cash_usd, cash, loan1, loan2, usd_twd
    )
    exp_tw = tw_m["cur_val"] * 2
    exp_us = us_m["exp_usd"] * usd_twd
    exp_total = (exp_tw + exp_us) / FC * 100    # ✅ 提前算好，不再藏在 metric 裡

    # ==========================================
    tab1, tab2, tab3 = st.tabs(["💰 台股", "💵 美股", "🛬 生命周期 & 退休"])

    # ------------------------------------------
    # 📈 Tab 1: 台股
    # ------------------------------------------
    with tab1:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("總市值", f"NT$ {tw_m['cur_val']:,.0f}")
        c2.metric("總投入成本", f"NT$ {tw_port['cost']:,.0f}")
        c3.metric("未實現總損益", f"{tw_m['cur_val'] - tw_port['cost']:+,.0f}", f"{tw_m['roi']*100:+.2f}%")
        c4.metric("今日損益", f"NT$ {tw_m['today_pnl']:+,.0f}", f"{tw_m['today_pct']:+.2f}%")
        c5.metric("實際曝險度", f"{exp_tw / FC * 100:.1f}%")

        c6, c7, c8, c9, c10 = st.columns(5)
        c6.metric("庫存總股數", f"{tw_port['shares']:,.0f} 股")
        c7.metric("持有均價", f"{tw_m['avg_cost']:.2f}" if tw_port["shares"] > 0 else "0")
        c8.metric("昨日還原收盤", f"{p_tw_yest:.2f}")
        c9.metric("目前現價", f"{p_tw_curr:.2f}")
        c10.metric("年化報酬率", f"{tw_m['ann_roi']:+.2f}%")

        st.write("---")
        col_p, col_d = st.columns([2, 1])
        with col_p:
            st.write("📈 **台幣資產配置比例 (含負債對照)**")
            fig_p = go.Figure(data=[go.Pie(
                labels=["00631L 市值", "可用現金", "信貸總餘額 (負債)"],
                values=[tw_m["cur_val"], cash, loan1 + loan2],
                hole=0.4,
                texttemplate="%{label}<br>NT$ %{value:,.0f}<br>%{percent}",
                marker_colors=["#E71D36", "#2EC4B6", "#5C5C5C"],
            )])
            fig_p.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_p, use_container_width=True)

        with st.expander(f"📜 逐筆投資戰績表 (目前現價: {p_tw_curr:.2f})", expanded=False):
            if not df_tw_raw.empty:
                buy_tw = df_tw_raw[df_tw_raw["交易類型"].str.contains("買入", na=False)].copy()
                buy_tw["成交日期"] = pd.to_datetime(buy_tw["成交日期"])
                recs_tw = []
                for _, r in buy_tw.sort_values("成交日期", ascending=False).iterrows():
                    # ✅ 用日期判斷分割還原
                    adj_p = adjust_price_for_split(r["成交價格"], r["成交日期"])
                    adj_s = adjust_shares_for_split(r["庫存股數"], r["成交日期"])
                    l_pnl = adj_s * p_tw_curr - r["持有成本"]
                    l_roi = l_pnl / r["持有成本"] if r["持有成本"] > 0 else 0
                    l_ann = ((1 + l_roi) ** (365 / max((datetime.today() - r["成交日期"]).days, 1)) - 1) * 100
                    recs_tw.append({
                        "日期": r["成交日期"].strftime("%Y-%m-%d"),
                        "買價": f"{adj_p:.2f}",
                        "股數": f"{adj_s:,.0f}",
                        "目前現價": f"{p_tw_curr:.2f}",
                        "今日損益": f"{(p_tw_curr - p_tw_yest) * adj_s:+,.0f}",
                        "總損益": f"{l_pnl:+,.0f}",
                        "年化報酬": f"{l_ann:+.1f}%",
                        "總報酬": f"{l_roi*100:+.1f}%",
                    })
                st.dataframe(pd.DataFrame(recs_tw), use_container_width=True, hide_index=True)

        st.write("---")
        st.link_button("🛒 新增台股交易紀錄 (直接開啟 Google Sheets 手動填寫)", SHEET_TW, use_container_width=True)

        st.subheader("🌐 戰術圖表分析")
        start_date = tw_port["min_date"] if pd.notnull(tw_port["min_date"]) else pd.to_datetime("2024-01-01")
        rp = adj_hist_tw[adj_hist_tw.index >= start_date]

        if not rp.dropna().empty:
            avg_cost = tw_m["avg_cost"]

            # 圖 A
            st.write("📈 **A. 價格走勢與還原均價**")
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(x=rp.index, y=rp.values, name="還原價", line=dict(color="#E71D36")))
            mx, mi, lt = rp.max(), rp.min(), rp.dropna().iloc[-1]
            if avg_cost > 0:
                fig1.add_hline(y=avg_cost, line_dash="dash", line_color="#00A86B", annotation_text=f"🟢 均價線: {avg_cost:.2f}")
                fig1.add_hrect(y0=avg_cost, y1=max(mx * 1.1, avg_cost * 1.1), fillcolor="green", opacity=0.1, layer="below", line_width=0)
                fig1.add_hrect(y0=min(mi * 0.9, avg_cost * 0.9), y1=avg_cost, fillcolor="red", opacity=0.1, layer="below", line_width=0)
            fig1.add_annotation(x=rp.idxmax(), y=mx, text=f"高:{mx:.2f}", showarrow=True, ay=-30)
            fig1.add_annotation(x=rp.idxmin(), y=mi, text=f"低:{mi:.2f}", showarrow=True, ay=30)
            fig1.add_annotation(x=rp.index[-1], y=lt, text=f"最新:{lt:.2f}", showarrow=True, ax=40)
            fig1.update_yaxes(range=[min(mi * 0.9, avg_cost * 0.9), max(mx * 1.1, avg_cost * 1.1)])
            st.plotly_chart(fig1, use_container_width=True)

            # 圖 B
            st.write("📊 **B. 多空戰術乖離率**")
            bias = (rp - rp.rolling(20).mean()) / rp.rolling(20).mean() * 100
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(x=bias.index, y=bias.values, name="乖離%", line=dict(color="#F4A261")))
            for v, c, t in [(-5, "gray", "標準 (-5)"), (-10, "orange", "恐慌 (-10)"), (-15, "red", "重壓 (-15)")]:
                fig2.add_hline(y=v, line_dash="dot", line_color=c, annotation_text=t)
            bc = bias.dropna()
            if not bc.empty:
                bx, bi, bl = bc.max(), bc.min(), bc.iloc[-1]
                fig2.add_hrect(y0=0, y1=max(bx * 1.2, 10), fillcolor="green", opacity=0.1, layer="below", line_width=0)
                fig2.add_hrect(y0=min(bi * 1.2, -20), y1=0, fillcolor="red", opacity=0.1, layer="below", line_width=0)
                fig2.add_annotation(x=bc.idxmax(), y=bx, text=f"最高:{bx:.1f}%", showarrow=True, ay=-30)
                fig2.add_annotation(x=bc.idxmin(), y=bi, text=f"最低:{bi:.1f}%", showarrow=True, ay=30)
                fig2.add_annotation(x=bc.index[-1], y=bl, text=f"最新:{bl:.1f}%", showarrow=True, ax=40)
                fig2.update_yaxes(range=[min(bi * 1.2, -20), max(bx * 1.2, 15)])
                st.plotly_chart(fig2, use_container_width=True)

            # 圖 C
            st.write("💰 **C. 庫存真實損益軌跡**")
            temp_tw = tw_port["df"]
            if not temp_tw.empty:
                th = temp_tw.groupby("成交日期")[["庫存股數", "持有成本"]].sum().reset_index().set_index("成交日期")
                dh = th.reindex(rp.index).fillna(0)
                ds = dh["庫存股數"].cumsum()
                dc = dh["持有成本"].cumsum()
                dp = np.where(dc > 0, (ds * rp - dc) / dc * 100, 0)
                dp_s = pd.Series(dp, index=rp.index)
                fig3 = go.Figure()
                fig3.add_trace(go.Scatter(x=dp_s.index, y=dp_s.values, line=dict(color="#247BA0")))
                dc_cl = dp_s.dropna()
                if not dc_cl.empty:
                    px, pi, pl = dc_cl.max(), dc_cl.min(), dc_cl.iloc[-1]
                    fig3.add_hrect(y0=0, y1=max(px * 1.2, 10), fillcolor="green", opacity=0.1, layer="below", line_width=0)
                    fig3.add_hrect(y0=min(pi * 1.2, -10), y1=0, fillcolor="red", opacity=0.1, layer="below", line_width=0)
                    fig3.add_annotation(x=dc_cl.idxmax(), y=px, text=f"最高:{px:.1f}%", showarrow=True, ay=-30)
                    fig3.add_annotation(x=dc_cl.idxmin(), y=pi, text=f"最低:{pi:.1f}%", showarrow=True, ay=30)
                    fig3.add_annotation(x=dc_cl.index[-1], y=pl, text=f"最新:{pl:.1f}%", showarrow=True, ax=40)
                    fig3.update_yaxes(range=[min(pi * 1.2, -15), max(px * 1.2, 20)])
                    st.plotly_chart(fig3, use_container_width=True)

    # ------------------------------------------
    # 🦅 Tab 2: 美股
    # ------------------------------------------
    with tab2:
        # SOXX 技術指標（保留原型指數分析）
        s_c = float(us_data["Close"]["SOXX"].iloc[-1])
        s_d = us_data["Close"]["SOXX"].rolling(100).mean().iloc[-1]
        soxl_c = us_live["SOXL"]["curr"]
        soxl_pred = soxl_c * (1 + (s_d / s_c - 1) * 3)

        st.markdown(f"### **SOXX 多頭續抱 | 現價:{s_c:.2f} (100DMA:{s_d:.2f} | 差距: {s_c - s_d:+.2f} / {(s_c/s_d - 1)*100:+.2f}%)**")
        st.info(f"💡 **預估 SOXL 壓力位：** 若 SOXX 跌回 100DMA，SOXL 預計來到 **${soxl_pred:.2f}** (距現值 {((soxl_pred/soxl_c - 1)*100):.1f}%)")
        cols = st.columns(3)
        for i, (l, t) in enumerate(zip([3, 4, 5], [30.14, 21.09, 14.77])):
            dist = (soxl_c / t - 1) * 100
            cols[i].metric(f"階梯 {l} 目標", f"${t}", f"距 {dist:.1f}%", delta_color="inverse")
        st.divider()

        u1, u2, u3, u4, u5 = st.columns(5)
        u1.metric("總市值 (USD)", f"${total_us_val_usd:,.2f}")
        u2.metric("總投入成本", f"${total_us_cost_usd:,.2f}")
        u3.metric("未實現總損益", f"{(total_us_val_usd - total_us_cost_usd):+,.2f}", f"{us_m['us_roi']*100:+.2f}%")
        u4.metric("今日損益", f"${us_m['today_pnl']:+,.2f}", f"{us_m['today_pct']*100:+.2f}%")
        u5.metric("實際曝險度", f"{us_m['exp_usd'] * usd_twd / FC * 100:.1f}%")

        st.write("---")
        st.write("📈 **美金資產配置比例 (USD)**")
        us_labels = list(us_live.keys()) + ["美股可用現金"]
        us_values = [info["curr"] * info["shares"] for info in us_live.values()] + [us_cash_usd]
        fig_u = go.Figure(data=[go.Pie(labels=us_labels, values=us_values, hole=0.4, texttemplate="%{label}<br>$%{value:,.0f}<br>%{percent}")])
        fig_u.update_layout(height=350, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig_u, use_container_width=True)

        st.subheader("📦 個股明細")
        us_table = []
        for t, info in us_live.items():
            avg = info["cost"] / info["shares"] if info["shares"] > 0 else 0
            l_roi = (info["curr"] / avg - 1) if avg > 0 else 0
            days = (datetime.today() - info["first_date"]).days if pd.notnull(info["first_date"]) else 1
            l_ann = ((1 + l_roi) ** (365 / max(days, 1)) - 1) * 100
            today_pnl_abs = (info["curr"] - info["yest"]) * info["shares"]
            total_pnl_abs = (info["curr"] - avg) * info["shares"]
            us_table.append({
                "代號": t,
                "股數": f"{info['shares']:,.0f}",
                "均價": f"${avg:.2f}",
                "成本": f"${info['cost']:,.0f}",
                "昨日收盤": f"${info['yest']:.2f}",
                "目前現價": f"${info['curr']:.2f}",
                "今日損益": f"${today_pnl_abs:+,.2f} ({(info['curr']/info['yest']-1)*100:+.2f}%)" if info["yest"] > 0 else "$0 (0.00%)",
                "總損益": f"${total_pnl_abs:+,.2f} ({l_roi*100:+.2f}%)",
                "年化報酬": f"{l_ann:+.2f}%",
            })
        st.dataframe(pd.DataFrame(us_table), use_container_width=True, hide_index=True)

        st.write("---")
        st.link_button("🛒 新增美股交易紀錄 (直接開啟 Google Sheets 手動填寫)", SHEET_US, use_container_width=True)

    # ------------------------------------------
    # 🛬 Tab 3: 生命周期 & 退休
    # ------------------------------------------
    with tab3:
        st.subheader("⚖️ 生命周期曝險透視")

        col_p1, col_p2 = st.columns(2)
        col_p1.metric("📈 台股投資組合佔比", f"{(tw_m['cur_val'] / (tw_m['cur_val'] + total_us_val_twd) * 100):.1f}%", "佔總持股比例")
        col_p2.metric("🦅 美股投資組合佔比", f"{(total_us_val_twd / (tw_m['cur_val'] + total_us_val_twd) * 100):.1f}%", "佔總持股比例")

        st.markdown(f"""
        | 戰區 | 曝險金額 (台幣) | 淨資產 (FC) | 實際曝險度 | 美金原值對照 |
        | :--- | :--- | :--- | :--- | :--- |
        | 📈 台股 | NT$ {exp_tw/10000:,.0f} 萬 | NT$ {(tw_m['cur_val']+cash/2-(loan1+loan2)/2)/10000:,.0f} 萬 | **{(exp_tw/FC*100):.1f}%** | - |
        | 🦅 美股 | NT$ {exp_us/10000:,.0f} 萬 | NT$ {(total_us_val_twd+cash/2-(loan1+loan2)/2)/10000:,.0f} 萬 | **{(exp_us/FC*100):.1f}%** | 曝險: **${us_m['exp_usd']:,.0f}** <br> 淨值: **${total_us_val_usd:,.0f}** |
        | 🔥 **總計** | **NT$ {(exp_tw+exp_us)/10000:,.0f} 萬** | **NT$ {FC/10000:,.0f} 萬** | **{exp_total:.1f}%** | (匯率: {usd_twd}) |
        """)

        W = FC + (base_m * 12 * hc_years)
        target_val = W * (target_k / 100)
        target_E = target_val / FC * 100

        # ✅ exp_total 已提前算好，直接使用
        c_tgt, c_act = st.columns(2)
        c_tgt.metric("🎯 生命周期目標曝險度", f"{target_E:.1f}%")
        c_act.metric("🔥 現在總曝險度", f"{exp_total:.1f}%", f"差距: {(exp_total - target_E):+.1f}%")

        st.subheader("⚖️ 應該如何平衡？")
        diff_val = (exp_tw + exp_us) - target_val
        if diff_val > 0:
            st.error(f"🚨 **目前總曝險過高！** 建議減少市場部位總價值約 **NT$ {diff_val/10000:,.0f} 萬**")
            st.write(f"👉 **台股部分：** 若由台股調整，需減碼 00631L 約 NT$ {diff_val/2/10000:,.1f} 萬市值")
            st.write(f"👉 **美股部分：** 若由美股調整，需減碼 SOXL 約 NT$ {diff_val/3/10000:,.1f} 萬市值")
        else:
            st.success(f"🟢 **目前曝險尚有空間！** 可增加市場部位約 **NT$ {abs(diff_val)/10000:,.0f} 萬**")

        st.divider()
        st.subheader("☕ 退休終局與提領反推")
        f_a = FC
        for _ in range(hc_years):
            f_a = f_a * 1.08 + (base_m * 12)
        m_a = (f_a * withdrawal_rate) / 12
        m_a_now = m_a / ((1 + inflation_rate) ** hc_years)
        st.markdown(f"**📈 情境 A：若工作 {hc_years} 年後退休**")
        ca1, ca2, ca3 = st.columns(3)
        ca1.metric("屆時滾出資產", f"NT$ {f_a/10000:,.0f} 萬")
        ca2.metric("未來每月可領", f"NT$ {m_a:,.0f}")
        ca3.metric("約等同現在每月可領", f"NT$ {m_a_now:,.0f}")

        st.write("")
        st.markdown(f"**🎯 情境 B：反推我想要月領 {target_monthly_now/10000:.0f} 萬(現值) 的退休金**")
        found_y = None
        t_f = FC
        for y in range(1, 41):
            t_f = t_f * 1.08 + (base_m * 12)
            req_m = target_monthly_now * ((1 + inflation_rate) ** y)
            if t_f >= (req_m * 12) / withdrawal_rate:
                found_y = y
                final_f = t_f
                final_m = req_m
                break
        if found_y:
            cb1, cb2, cb3 = st.columns(3)
            cb1.metric("需滾出資產", f"NT$ {final_f/10000:,.0f} 萬")
            cb2.metric("未來每月可領", f"NT$ {final_m:,.0f}")
            cb3.metric("剩餘年限", f"{found_y} 年")

        with st.expander("🛬 降落時程推演表 (Glide Path)", expanded=False):
            gp = []
            curr_f = FC
            for y in range(hc_years + 1):
                if y > 0:
                    curr_f = curr_f * 1.08 + (base_m * 12)
                h_r = max(0, (base_m * 12 * hc_years) - (base_m * 12 * y))
                e_g = ((curr_f + h_r) * target_k / 100) / curr_f * 100
                gp.append({"年": f"第 {y} 年", "預估資產(萬)": f"{curr_f/10000:,.0f}", "應有曝險": f"{e_g:.1f}%"})
            st.table(pd.DataFrame(gp))

st.caption("📱 提示：點擊下方按鈕可直接跳轉至試算表新增交易紀錄。V8.8 架構重構版。")
