import streamlit as st
import pandas as pd
from supabase import create_client

# ========================
# 페이지 설정
# ========================

st.set_page_config(
    page_title="생산 관리 시스템",
    page_icon="🏠",
    layout="wide"
)

# ========================
# Supabase 연결
# ========================

@st.cache_resource
def get_supabase_client():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase_client()

# ========================
# 메인 홈 화면 (함수로 정의)
# ========================

@st.cache_data(ttl=120)
def _load_home_schedule_summary():
    """홈 화면용 스케줄 요약 (캐시 2분)"""
    result = supabase.table("schedules").select(
        "week_start, week_end, product, quantity, production_time"
    ).order("week_start", desc=True).limit(500).execute()
    if not result.data:
        return None, None
    latest_week = result.data[0]["week_start"]
    latest_end = result.data[0]["week_end"]
    stats = [r for r in result.data if r["week_start"] == latest_week]
    return {"week_start": latest_week, "week_end": latest_end}, stats

@st.cache_data(ttl=120)
def _load_home_product_summary():
    """홈 화면용 제품 요약 (캐시 2분)"""
    result = supabase.table("products").select("category, used_raw_meat").execute()
    return result.data if result.data else []

@st.cache_data(ttl=120)
def _load_home_sales_summary():
    """홈 화면용 판매 요약 (캐시 2분) — 단일 쿼리로 최적화"""
    count_result = supabase.table("sales").select("id", count="exact").execute()
    total_count = count_result.count or 0
    if total_count == 0:
        return 0, None, None, 0
    latest = supabase.table("sales").select("sale_date").order("sale_date", desc=True).limit(1).execute()
    earliest = supabase.table("sales").select("sale_date").order("sale_date", desc=False).limit(1).execute()
    latest_date = latest.data[0]["sale_date"] if latest.data else None
    earliest_date = earliest.data[0]["sale_date"] if earliest.data else None
    # 고유 날짜 수: 최대 1000건만 조회하여 추정 (전체 페이지네이션 제거)
    dates_result = supabase.table("sales").select("sale_date").order("sale_date").limit(1000).execute()
    unique_dates = set(row["sale_date"] for row in dates_result.data) if dates_result.data else set()
    return total_count, latest_date, earliest_date, len(unique_dates)

def home_page():
    st.title("📊 생산 관리 시스템")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📅 스케줄 관리")
        st.caption("주간 생산 스케줄을 생성하고 조회합니다.")

        try:
            schedule_info, schedule_stats = _load_home_schedule_summary()

            if schedule_info:
                st.success(f"최근 스케줄: **{schedule_info['week_start']} ~ {schedule_info['week_end']}**")

                if schedule_stats:
                    df = pd.DataFrame(schedule_stats)
                    m1, m2, m3 = st.columns(3)
                    with m1:
                        st.metric("총 생산량", f"{df['quantity'].sum()}개")
                    with m2:
                        st.metric("제품 종류", f"{df['product'].nunique()}개")
                    with m3:
                        st.metric("총 생산시간", f"{df['production_time'].sum():.1f}h")
            else:
                st.info("등록된 스케줄이 없습니다.")
        except:
            st.info("등록된 스케줄이 없습니다.")

    with col2:
        st.subheader("📦 제품 관리")
        st.caption("제품코드, 제품명, 사용원육, 분류를 관리합니다.")

        try:
            product_data = _load_home_product_summary()

            if product_data:
                df = pd.DataFrame(product_data)
                m1, m2, m3 = st.columns(3)
                with m1:
                    st.metric("등록 제품", f"{len(df)}개")
                with m2:
                    cats = df.get("category", pd.Series(dtype=object))
                    unique_cats = cats.dropna().astype(str).str.strip()
                    unique_cats = unique_cats[unique_cats != ""].nunique()
                    st.metric("분류 수", f"{unique_cats}개")
                with m3:
                    meats = df.get("used_raw_meat", pd.Series(dtype=object))
                    unique_meats = meats.dropna().astype(str).str.strip()
                    unique_meats = unique_meats[unique_meats != ""].nunique()
                    st.metric("사용원육 종류", f"{unique_meats}개")
            else:
                st.info("등록된 제품이 없습니다.")
        except:
            st.info("등록된 제품이 없습니다.")

    # 판매 데이터 요약
    st.divider()
    st.subheader("📊 판매 데이터")
    st.caption("Supabase에 저장된 판매량 데이터 현황입니다.")

    try:
        total_count, latest_date, earliest_date, unique_date_count = _load_home_sales_summary()

        if total_count > 0:
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("등록 날짜 수", f"{unique_date_count}일")
            with m2:
                if latest_date:
                    st.metric("최근 데이터", f"{latest_date}")
            with m3:
                st.metric("총 데이터 건수", f"{total_count:,}건")
        else:
            st.info("등록된 판매 데이터가 없습니다. '판매 데이터' 페이지에서 업로드해주세요.")
    except:
        st.info("등록된 판매 데이터가 없습니다.")

# ========================
# 네비게이션
# ========================

home = st.Page(home_page, title="메인 홈", icon="🏠", default=True)
schedule = st.Page("views/schedule.py", title="스케줄 관리", icon="📅")
products = st.Page("views/products/products_main.py", title="제품 관리", icon="📦")
sales = st.Page("views/sales.py", title="판매 데이터", icon="📊")

pg = st.navigation([home, schedule, products, sales])
pg.run()

# ========================
# 공통 사이드바 (모든 페이지에 표시)
# ========================
st.sidebar.divider()
st.sidebar.caption("v1.4.0 | 생산 관리 시스템 (Supabase)")
