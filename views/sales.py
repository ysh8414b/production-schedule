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

@st.cache_data(ttl=120)
def load_sales_all(date_from=None, date_to=None):
    """페이지네이션으로 전체 데이터 조회 (1000건 제한 우회, 캐시 2분)"""
    all_data = []
    page_size = 1000
    offset = 0
    
    while True:
        query = supabase.table("sales").select("*").order("sale_date", desc=True).order("product_name")
        if date_from:
            query = query.gte("sale_date", date_from)
        if date_to:
            query = query.lte("sale_date", date_to)
        query = query.range(offset, offset + page_size - 1)
        result = query.execute()
        
        if not result.data:
            break
        all_data.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    
    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame(columns=["id", "sale_date", "product_code", "product_name", "quantity"])

def insert_sales_bulk(rows):
    """판매 데이터 일괄 등록 (500건씩 나눠서)"""
    chunk_size = 500
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        supabase.table("sales").insert(chunk).execute()

def delete_sales_by_date_range(date_from, date_to):
    """기간별 판매 데이터 삭제"""
    supabase.table("sales").delete().gte(
        "sale_date", date_from
    ).lte(
        "sale_date", date_to
    ).execute()
    # 캐시 클리어
    load_sales_all.clear()
    get_sales_date_range.clear()
    get_sales_count.clear()

@st.cache_data(ttl=120)
def get_sales_date_range():
    """등록된 판매 데이터의 날짜 범위 조회 (캐시 2분)"""
    result = supabase.table("sales").select("sale_date").order("sale_date").limit(1).execute()
    if result.data:
        min_date = result.data[0]["sale_date"]
        result2 = supabase.table("sales").select("sale_date").order("sale_date", desc=True).limit(1).execute()
        max_date = result2.data[0]["sale_date"]
        return min_date, max_date
    return None, None

@st.cache_data(ttl=120)
def get_sales_count(date_from=None, date_to=None):
    """판매 데이터 총 건수 조회 (캐시 2분)"""
    query = supabase.table("sales").select("id", count="exact")
    if date_from:
        query = query.gte("sale_date", date_from)
    if date_to:
        query = query.lte("sale_date", date_to)
    result = query.execute()
    return result.count or 0

# ========================
# 메인 앱
# ========================

st.title("📊 판매 데이터 관리")
st.caption("판매량 데이터를 업로드하고 관리합니다.")

menu = st.radio("선택", [
    "📋 데이터 조회",
    "📤 엑셀 업로드",
    "📥 엑셀 다운로드",
    "🗑️ 데이터 삭제"
], horizontal=True)

st.divider()

# 업로드 완료 메시지 표시 (rerun 후에도 보이도록)
if st.session_state.get("upload_success"):
    st.success(st.session_state["upload_success"])
    st.session_state["upload_success"] = None

if menu == "📋 데이터 조회":
    st.header("판매 데이터 조회")
    
    date_range = get_sales_date_range()
    
    if date_range[0] is None:
        st.info("등록된 판매 데이터가 없습니다. '엑셀 업로드'로 데이터를 추가해주세요.")
    else:
        st.success(f"등록 기간: **{date_range[0]}** ~ **{date_range[1]}**")
        
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("시작일", pd.to_datetime(date_range[0]))
        with col2:
            to_date = st.date_input("종료일", pd.to_datetime(date_range[1]))
        
        total_count = get_sales_count(
            from_date.strftime('%Y-%m-%d'),
            to_date.strftime('%Y-%m-%d')
        )
        
        if total_count == 0:
            st.info("해당 기간에 데이터가 없습니다.")
        else:
            df = load_sales_all(
                from_date.strftime('%Y-%m-%d'),
                to_date.strftime('%Y-%m-%d')
            )
            
            search = st.text_input("🔍 제품 검색", placeholder="제품명 또는 제품코드 입력...")
            
            if search:
                df = df[
                    df["product_name"].str.contains(search, case=False, na=False) |
                    df["product_code"].str.contains(search, case=False, na=False)
                ]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("데이터 건수", f"{len(df):,}건")
            with col2:
                st.metric("제품 종류", f"{df['product_name'].nunique()}개")
            with col3:
                st.metric("총 판매량", f"{df['quantity'].sum():,}개")
            
            st.divider()
            
            display_df = df[["sale_date", "product_code", "product_name", "quantity"]].rename(columns={
                "sale_date": "날짜",
                "product_code": "제품코드",
                "product_name": "제품명",
                "quantity": "수량"
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)

elif menu == "📤 엑셀 업로드":
    st.header("엑셀로 판매 데이터 등록")
    
    st.info("""
    **엑셀 파일 형식:**
    | 날짜 | 제품코드 | 제품명 | 수량 |
    |------|---------|--------|------|
    | 2025-01-06 | P001 | 제품A | 50 |
    | 2025-01-06 | P002 | 제품B | 30 |
    
    - 첫 번째 행은 헤더여야 합니다
    - 같은 날짜+제품이 여러 건 있어도 그대로 등록됩니다
    - 날짜 컬럼: 날짜, date, 일자 등
    - 제품코드 컬럼: 제품코드, code, 코드 등
    - 제품명 컬럼: 제품명, 제품, name 등
    - 수량 컬럼: 수량, 판매량, quantity 등
    """)
    
    uploaded = st.file_uploader("📁 엑셀 파일 업로드", type=["xlsx"])
    
    if uploaded:
        try:
            df = pd.read_excel(uploaded)
            
            # 컬럼 자동 매핑
            col_map = {}
            for col in df.columns:
                col_lower = str(col).lower().replace(" ", "")
                if "날짜" in col_lower or "date" in col_lower or "일자" in col_lower:
                    col_map[col] = "sale_date"
                elif "코드" in col_lower or "code" in col_lower:
                    col_map[col] = "product_code"
                elif "제품" in col_lower or "품목" in col_lower or "name" in col_lower or "이름" in col_lower:
                    if "코드" not in col_lower and "code" not in col_lower:
                        col_map[col] = "product_name"
                elif "수량" in col_lower or "판매" in col_lower or "quantity" in col_lower or "qty" in col_lower:
                    col_map[col] = "quantity"
            
            df = df.rename(columns=col_map)
            
            # 필수 컬럼 확인
            required = ["sale_date", "product_name", "quantity"]
            missing = [c for c in required if c not in df.columns]
            
            if missing:
                st.error(f"필수 컬럼이 없습니다: {missing}")
                st.caption("날짜, 제품명, 수량 컬럼이 반드시 포함되어야 합니다.")
            else:
                # 제품코드 없으면 제품명으로 대체
                if "product_code" not in df.columns:
                    df["product_code"] = df["product_name"]
                
                # 날짜 형식 처리: "25/11/01 (Sat)" 같은 형식 대응
                def parse_date(val):
                    val = str(val).strip()
                    if "(" in val:
                        val = val[:val.index("(")].strip()
                    for fmt in ["%y/%m/%d", "%Y/%m/%d", "%Y-%m-%d", "%y-%m-%d", "%Y.%m.%d", "%y.%m.%d", "%m/%d/%Y", "%m/%d/%y"]:
                        try:
                            return pd.to_datetime(val, format=fmt)
                        except:
                            continue
                    return pd.to_datetime(val)
                
                df["sale_date"] = df["sale_date"].apply(parse_date).dt.strftime('%Y-%m-%d')
                df["quantity"] = df["quantity"].fillna(0).astype(int)
                df["product_code"] = df["product_code"].astype(str).str.strip()
                df["product_name"] = df["product_name"].astype(str).str.strip()
                df = df.dropna(subset=["sale_date", "product_name"])
                
                st.subheader("📋 미리보기")
                preview = df[["sale_date", "product_code", "product_name", "quantity"]].rename(columns={
                    "sale_date": "날짜",
                    "product_code": "제품코드",
                    "product_name": "제품명",
                    "quantity": "수량"
                })
                st.dataframe(preview, use_container_width=True, hide_index=True)
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.caption(f"총 {len(df):,}건")
                with col2:
                    st.caption(f"기간: {df['sale_date'].min()} ~ {df['sale_date'].max()}")
                with col3:
                    st.caption(f"제품: {df['product_name'].nunique()}종")
                
                col_btn1, col_btn2 = st.columns([1, 4])
                with col_btn1:
                    if st.button("🚀 등록", type="primary"):
                        rows = df[["sale_date", "product_code", "product_name", "quantity"]].to_dict("records")
                        
                        progress = st.progress(0, text="업로드 중...")
                        chunk_size = 500
                        total_chunks = (len(rows) + chunk_size - 1) // chunk_size
                        
                        for i in range(0, len(rows), chunk_size):
                            chunk = rows[i:i + chunk_size]
                            supabase.table("sales").insert(chunk).execute()
                            current_chunk = (i // chunk_size) + 1
                            progress.progress(
                                current_chunk / total_chunks,
                                text=f"업로드 중... ({min(i + chunk_size, len(rows)):,}/{len(rows):,}건)"
                            )
                        
                        progress.progress(1.0, text="완료!")
                        # 캐시 클리어
                        load_sales_all.clear()
                        get_sales_date_range.clear()
                        get_sales_count.clear()
                        st.session_state["upload_success"] = f"✅ {len(rows):,}건 업로드 완료!"
                        st.rerun()
        
        except Exception as e:
            st.error(f"❌ 파일 처리 오류: {str(e)}")

elif menu == "📥 엑셀 다운로드":
    st.header("판매 데이터 다운로드")
    
    date_range = get_sales_date_range()
    
    if date_range[0] is None:
        st.info("등록된 판매 데이터가 없습니다.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("시작일", pd.to_datetime(date_range[0]))
        with col2:
            to_date = st.date_input("종료일", pd.to_datetime(date_range[1]))
        
        total_count = get_sales_count(
            from_date.strftime('%Y-%m-%d'),
            to_date.strftime('%Y-%m-%d')
        )
        
        if total_count == 0:
            st.info("해당 기간에 데이터가 없습니다.")
        else:
            st.caption(f"총 {total_count:,}건")
            
            if st.button("📥 데이터 불러오기"):
                df = load_sales_all(
                    from_date.strftime('%Y-%m-%d'),
                    to_date.strftime('%Y-%m-%d')
                )
                
                display_df = df[["sale_date", "product_code", "product_name", "quantity"]].rename(columns={
                    "sale_date": "날짜",
                    "product_code": "제품코드",
                    "product_name": "제품명",
                    "quantity": "수량"
                })
                st.dataframe(display_df, use_container_width=True, hide_index=True)
                
                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    display_df.to_excel(writer, index=False, sheet_name="판매데이터")
                
                st.download_button(
                    label="💾 Excel 다운로드",
                    data=output.getvalue(),
                    file_name=f"판매데이터_{from_date}_{to_date}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

elif menu == "🗑️ 데이터 삭제":
    st.header("판매 데이터 삭제")
    
    date_range = get_sales_date_range()
    
    if date_range[0] is None:
        st.info("등록된 판매 데이터가 없습니다.")
    else:
        st.warning("⚠️ 삭제된 데이터는 복구할 수 없습니다.")
        
        col1, col2 = st.columns(2)
        with col1:
            from_date = st.date_input("삭제 시작일", pd.to_datetime(date_range[0]))
        with col2:
            to_date = st.date_input("삭제 종료일", pd.to_datetime(date_range[1]))
        
        total_count = get_sales_count(
            from_date.strftime('%Y-%m-%d'),
            to_date.strftime('%Y-%m-%d')
        )
        
        if total_count > 0:
            st.caption(f"삭제 대상: **{total_count:,}건**")
            
            if st.button("🗑️ 삭제", type="primary"):
                st.session_state["confirm_delete_sales"] = True
            
            if st.session_state.get("confirm_delete_sales"):
                st.error(f"정말로 {from_date} ~ {to_date} 기간의 {total_count:,}건을 삭제하시겠습니까?")
                col_a, col_b, _ = st.columns([1, 1, 4])
                with col_a:
                    if st.button("✅ 삭제 확인", key="confirm_del_sales"):
                        delete_sales_by_date_range(
                            from_date.strftime('%Y-%m-%d'),
                            to_date.strftime('%Y-%m-%d')
                        )
                        st.success("✅ 삭제 완료!")
                        st.session_state["confirm_delete_sales"] = False
                        st.rerun()
                with col_b:
                    if st.button("❌ 취소", key="cancel_del_sales"):
                        st.session_state["confirm_delete_sales"] = False
                        st.rerun()
        else:
            st.info("해당 기간에 데이터가 없습니다.")

