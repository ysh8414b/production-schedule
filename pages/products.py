import streamlit as st
import pandas as pd
from supabase import create_client
from io import BytesIO

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
# DB 함수
# ========================

def load_products():
    result = supabase.table("products").select("*").order("product_name").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame(columns=["id", "product_name", "production_time_sec", "loss_rate"])

def upsert_product(name, time_sec, loss_rate):
    supabase.table("products").upsert(
        {
            "product_name": name,
            "production_time_sec": int(time_sec),
            "loss_rate": float(loss_rate)
        },
        on_conflict="product_name"
    ).execute()

def upsert_products_bulk(rows):
    supabase.table("products").upsert(
        rows,
        on_conflict="product_name"
    ).execute()

def delete_product(product_id):
    supabase.table("products").delete().eq("id", product_id).execute()

# ========================
# 메인 앱
# ========================

st.title("📦 제품 관리")
st.caption("제품별 생산시간, 로스율을 관리합니다.")

menu = st.radio("선택", [
    "📋 제품 목록",
    "✏️ 제품 등록/수정",
    "📤 엑셀 업로드",
    "📥 엑셀 다운로드"
], horizontal=True)

st.divider()

if menu == "📋 제품 목록":
    st.header("등록된 제품 목록")
    
    df = load_products()
    
    if df.empty:
        st.info("등록된 제품이 없습니다. '제품 등록/수정' 또는 '엑셀 업로드'로 추가해주세요.")
    else:
        search = st.text_input("🔍 제품 검색", placeholder="제품명 입력...")
        
        if search:
            df = df[df["product_name"].str.contains(search, case=False, na=False)]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("총 제품 수", f"{len(df)}개")
        with col2:
            avg_time = df["production_time_sec"].mean()
            st.metric("평균 생산시간", f"{avg_time:.0f}초")
        with col3:
            avg_loss = df["loss_rate"].mean()
            st.metric("평균 로스율", f"{avg_loss:.1f}%")
        
        st.divider()
        
        display_df = df[["product_name", "production_time_sec", "loss_rate"]].rename(columns={
            "product_name": "제품명",
            "production_time_sec": "생산시간(초)",
            "loss_rate": "로스율(%)"
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.divider()
        st.subheader("🗑️ 제품 삭제")
        
        delete_target = st.selectbox(
            "삭제할 제품 선택",
            options=df["product_name"].tolist(),
            index=None,
            placeholder="제품을 선택하세요..."
        )
        
        if delete_target:
            col_a, col_b = st.columns([1, 4])
            with col_a:
                if st.button("🗑️ 삭제", type="primary"):
                    product_id = df[df["product_name"] == delete_target]["id"].iloc[0]
                    delete_product(product_id)
                    st.success(f"✅ '{delete_target}' 삭제 완료")
                    st.rerun()

elif menu == "✏️ 제품 등록/수정":
    st.header("제품 등록 / 수정")
    st.caption("이미 존재하는 제품명을 입력하면 자동으로 수정됩니다.")
    
    df = load_products()
    
    existing = st.selectbox(
        "기존 제품 수정 (새 제품이면 비워두세요)",
        options=[""] + df["product_name"].tolist(),
        index=0
    )
    
    if existing:
        row = df[df["product_name"] == existing].iloc[0]
        default_name = row["product_name"]
        default_time = int(row["production_time_sec"])
        default_loss = float(row["loss_rate"])
    else:
        default_name = ""
        default_time = 0
        default_loss = 0.0
    
    with st.form("product_form"):
        product_name = st.text_input("제품명", value=default_name)
        
        col1, col2 = st.columns(2)
        with col1:
            production_time = st.number_input(
                "개당 생산시간 (초)", 
                min_value=0, 
                max_value=9999,
                value=default_time,
                step=1
            )
        with col2:
            loss_rate = st.number_input(
                "로스율 (%)", 
                min_value=0.0, 
                max_value=100.0,
                value=default_loss,
                step=0.1,
                format="%.1f"
            )
        
        submitted = st.form_submit_button("💾 저장", type="primary")
        
        if submitted:
            if not product_name.strip():
                st.error("제품명을 입력해주세요.")
            else:
                upsert_product(product_name.strip(), production_time, loss_rate)
                st.success(f"✅ '{product_name}' 저장 완료!")
                st.rerun()

elif menu == "📤 엑셀 업로드":
    st.header("엑셀로 일괄 등록")
    
    st.info("""
    **엑셀 파일 형식:**
    | 제품명 | 생산시간(초) | 로스율(%) |
    |--------|-------------|----------|
    | 제품A  | 120         | 2.5      |
    | 제품B  | 90          | 1.8      |
    
    - 첫 번째 행은 헤더여야 합니다
    - 이미 존재하는 제품명은 자동으로 **덮어쓰기** 됩니다
    """)
    
    uploaded = st.file_uploader("📁 엑셀 파일 업로드", type=["xlsx"])
    
    if uploaded:
        try:
            df = pd.read_excel(uploaded)
            
            col_map = {}
            for col in df.columns:
                col_lower = str(col).lower().replace(" ", "")
                if "제품" in col_lower or "이름" in col_lower or "name" in col_lower:
                    col_map[col] = "product_name"
                elif "시간" in col_lower or "time" in col_lower or "초" in col_lower:
                    col_map[col] = "production_time_sec"
                elif "로스" in col_lower or "loss" in col_lower:
                    col_map[col] = "loss_rate"
            
            df = df.rename(columns=col_map)
            
            required = ["product_name"]
            missing = [c for c in required if c not in df.columns]
            
            if missing:
                st.error(f"필수 컬럼이 없습니다: {missing}. '제품명' 컬럼이 포함되어야 합니다.")
            else:
                if "production_time_sec" not in df.columns:
                    df["production_time_sec"] = 0
                if "loss_rate" not in df.columns:
                    df["loss_rate"] = 0.0
                
                df["production_time_sec"] = df["production_time_sec"].fillna(0).astype(int)
                df["loss_rate"] = df["loss_rate"].fillna(0.0).astype(float)
                df = df.dropna(subset=["product_name"])
                
                st.subheader("📋 미리보기")
                preview = df[["product_name", "production_time_sec", "loss_rate"]].rename(columns={
                    "product_name": "제품명",
                    "production_time_sec": "생산시간(초)",
                    "loss_rate": "로스율(%)"
                })
                st.dataframe(preview, use_container_width=True, hide_index=True)
                st.caption(f"총 {len(df)}개 제품")
                
                col1, col2 = st.columns([1, 4])
                with col1:
                    if st.button("🚀 등록", type="primary"):
                        rows = df[["product_name", "production_time_sec", "loss_rate"]].to_dict("records")
                        upsert_products_bulk(rows)
                        st.success(f"✅ {len(rows)}개 제품 등록 완료!")
                        st.rerun()
        
        except Exception as e:
            st.error(f"❌ 파일 처리 오류: {str(e)}")

elif menu == "📥 엑셀 다운로드":
    st.header("제품 목록 다운로드")
    
    df = load_products()
    
    if df.empty:
        st.info("등록된 제품이 없습니다.")
    else:
        st.caption(f"총 {len(df)}개 제품")
        
        display_df = df[["product_name", "production_time_sec", "loss_rate"]].rename(columns={
            "product_name": "제품명",
            "production_time_sec": "생산시간(초)",
            "loss_rate": "로스율(%)"
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            display_df.to_excel(writer, index=False, sheet_name="제품목록")
        
        st.download_button(
            label="💾 Excel 다운로드",
            data=output.getvalue(),
            file_name="제품목록.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

st.sidebar.divider()
st.sidebar.caption("v1.1.0 | 생산 관리 시스템 (Supabase)")
