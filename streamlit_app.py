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
    # 로고 이미지를 base64로 인라인 삽입
    import base64, os
    # 여러 경로 후보 시도
    candidates = [
        os.path.join(os.getcwd(), "assets", "logo.png"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "assets", "logo.png"),
        os.path.join("assets", "logo.png"),
    ]
    logo_b64 = None
    for logo_path in candidates:
        if os.path.exists(logo_path):
            with open(logo_path, "rb") as f:
                logo_b64 = base64.b64encode(f.read()).decode()
            break
    
    if logo_b64:
        st.markdown(
            f"""
            <div style="display:flex; align-items:center; gap:12px; margin-bottom:4px;">
                <img src="data:image/png;base64,{logo_b64}" style="height:48px; border-radius:6px;"/>
                <span style="font-size:32px; font-weight:700;">📊 생산 관리 시스템</span>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
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
        st.caption("제품코드, 제품명, 사용원육, 분류를 관리합니다.")
        
        try:
            result = supabase.table("products").select("*").execute()
            
            if result.data:
                df = pd.DataFrame(result.data)
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
        # 총 건수 조회
        count_result = supabase.table("sales").select("id", count="exact").execute()
        total_count = count_result.count or 0
        
        # 최근/최초 날짜
        latest = supabase.table("sales").select("sale_date").order("sale_date", desc=True).limit(1).execute()
        earliest = supabase.table("sales").select("sale_date").order("sale_date", desc=False).limit(1).execute()
        
        # 고유 날짜 수 (전체 페이지네이션)
        unique_dates = set()
        offset = 0
        page_size = 1000
        while True:
            dates_result = supabase.table("sales").select("sale_date").order("sale_date").range(offset, offset + page_size - 1).execute()
            if not dates_result.data:
                break
            for row in dates_result.data:
                unique_dates.add(row["sale_date"])
            if len(dates_result.data) < page_size:
                break
            offset += page_size
        
        if total_count > 0:
            m1, m2, m3 = st.columns(3)
            with m1:
                st.metric("등록 날짜 수", f"{len(unique_dates)}일")
            with m2:
                if latest.data:
                    st.metric("최근 데이터", f"{latest.data[0]['sale_date']}")
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
