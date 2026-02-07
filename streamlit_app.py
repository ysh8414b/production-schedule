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

def home_page():
    st.title("📊 생산 관리 시스템")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📅 스케줄 관리")
        st.caption("주간 생산 스케줄을 생성하고 조회합니다.")
        
        try:
            result = supabase.table("schedules").select(
                "week_start, week_end"
            ).order("week_start", desc=True).limit(1).execute()
            
            if result.data:
                latest = result.data[0]
                st.success(f"최근 스케줄: **{latest['week_start']} ~ {latest['week_end']}**")
                
                stats = supabase.table("schedules").select("*").eq(
                    "week_start", latest["week_start"]
                ).execute()
                
                if stats.data:
                    df = pd.DataFrame(stats.data)
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
        st.caption("제품별 생산시간, 로스율을 관리합니다.")
        
        try:
            result = supabase.table("products").select("*").execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
                m1, m2, m3 = st.columns(3)
                with m1:
                    st.metric("등록 제품", f"{len(df)}개")
                with m2:
                    st.metric("평균 생산시간", f"{df['production_time_sec'].mean():.0f}초")
                with m3:
                    st.metric("평균 로스율", f"{df['loss_rate'].mean():.1f}%")
            else:
                st.info("등록된 제품이 없습니다.")
        except:
            st.info("등록된 제품이 없습니다.")

    # 판매 데이터 요약
    st.divider()
    st.subheader("📊 판매 데이터")
    st.caption("Supabase에 저장된 판매량 데이터 현황입니다.")
    
    try:
        result = supabase.table("sales").select("sale_date, quantity").order("sale_date", desc=True).limit(1000).execute()
        
        if result.data:
            df = pd.DataFrame(result.data)
            dates = df["sale_date"].unique()
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("등록 날짜 수", f"{len(dates)}일")
            with m2:
                st.metric("최근 데이터", f"{sorted(dates)[-1]}")
            with m3:
                st.metric("총 데이터 건수", f"{len(df)}건")
        else:
            st.info("등록된 판매 데이터가 없습니다. '판매 데이터' 페이지에서 업로드해주세요.")
    except:
        st.info("등록된 판매 데이터가 없습니다.")

    st.sidebar.divider()
    st.sidebar.caption("v1.2.0 | 생산 관리 시스템 (Supabase)")

# ========================
# 네비게이션
# ========================

home = st.Page(home_page, title="메인 홈", icon="🏠", default=True)
schedule = st.Page("views/schedule.py", title="스케줄 관리", icon="📅")
products = st.Page("views/products.py", title="제품 관리", icon="📦")
sales = st.Page("views/sales.py", title="판매 데이터", icon="📊")

pg = st.navigation([home, schedule, products, sales])
pg.run()
