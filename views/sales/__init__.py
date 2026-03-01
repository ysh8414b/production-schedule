import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import date, timedelta

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
# 판매 데이터 DB 함수
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
# 투입 원육 DB 함수
# ========================

@st.cache_data(ttl=60)
def load_raw_meat_inputs():
    """raw_meat_inputs 테이블에서 투입 원육 로드"""
    try:
        result = supabase.table("raw_meat_inputs").select("*").order("move_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "move_date", "meat_code", "meat_name", "origin_grade",
        "kg", "move_amount", "tracking_number", "product_name", "production_kg", "memo", "completed"
    ])

def insert_raw_meat_inputs(rows):
    """원육 투입 데이터 일괄 등록"""
    supabase.table("raw_meat_inputs").insert(rows).execute()
    load_raw_meat_inputs.clear()

def update_raw_meat_input(row_id, data: dict):
    """원육 투입 데이터 수정"""
    supabase.table("raw_meat_inputs").update(data).eq("id", row_id).execute()
    load_raw_meat_inputs.clear()

def delete_raw_meat_input(row_id):
    """원육 투입 데이터 삭제"""
    supabase.table("raw_meat_inputs").delete().eq("id", row_id).execute()
    load_raw_meat_inputs.clear()

def cleanup_old_raw_meat_inputs():
    """2개월 이전 투입 원육 데이터 자동 삭제"""
    cutoff = (date.today() - timedelta(days=60)).strftime("%Y-%m-%d")
    try:
        supabase.table("raw_meat_inputs").delete().lt("move_date", cutoff).execute()
        load_raw_meat_inputs.clear()
    except:
        pass

# ========================
# 제품-원육 매핑 DB 함수
# ========================

@st.cache_data(ttl=120)
def load_product_rawmeats():
    """제품-원육 매핑 조회"""
    try:
        result = supabase.table("product_rawmeats").select("*").order("product_name").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["id", "product_name", "meat_code", "meat_name", "origin_grade"])

def upsert_product_rawmeat(product_name, meat_code, meat_name, origin_grade):
    """제품-원육 매핑 등록 (중복 시 무시)"""
    try:
        supabase.table("product_rawmeats").upsert(
            {
                "product_name": str(product_name).strip(),
                "meat_code": str(meat_code).strip(),
                "meat_name": str(meat_name).strip(),
                "origin_grade": str(origin_grade).strip(),
            },
            on_conflict="product_name,meat_code"
        ).execute()
        load_product_rawmeats.clear()
    except:
        pass

def delete_product_rawmeat(row_id):
    """제품-원육 매핑 삭제"""
    supabase.table("product_rawmeats").delete().eq("id", row_id).execute()
    load_product_rawmeats.clear()

# ========================
# 로스 할당 DB 함수 (loss_assignments)
# ========================

@st.cache_data(ttl=60)
def load_loss_assignments():
    """로스 할당 데이터 조회"""
    try:
        result = supabase.table("loss_assignments").select("*").order("move_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "raw_meat_input_id", "move_date", "meat_code", "meat_name",
        "origin_grade", "kg", "tracking_number",
        "product_name", "production_kg", "memo", "completed"
    ])

def insert_loss_assignment(data: dict):
    """로스 할당 레코드 생성"""
    supabase.table("loss_assignments").insert(data).execute()
    load_loss_assignments.clear()

def insert_loss_assignments_bulk(rows):
    """로스 할당 레코드 일괄 생성"""
    supabase.table("loss_assignments").insert(rows).execute()
    load_loss_assignments.clear()

def update_loss_assignment(row_id, data: dict):
    """로스 할당 레코드 수정"""
    result = supabase.table("loss_assignments").update(data).eq("id", row_id).execute()
    load_loss_assignments.clear()
    # 업데이트 실패 시 (RLS 등) 에러 발생
    if not result.data:
        raise Exception(f"업데이트 실패: id={row_id} 에 해당하는 레코드를 찾을 수 없거나 권한이 없습니다.")

def delete_loss_assignment(row_id):
    """로스 할당 레코드 삭제 (투입 원육 데이터는 유지)"""
    supabase.table("loss_assignments").delete().eq("id", row_id).execute()
    load_loss_assignments.clear()


def sync_product_rawmeats():
    """loss_assignments + production_status_items 기준으로 product_rawmeats 동기화"""
    active_mappings = {}

    # Source 1: loss_assignments (기존 레거시 데이터)
    load_loss_assignments.clear()
    loss_df = load_loss_assignments()
    if not loss_df.empty:
        completed = loss_df[
            (loss_df["completed"] == True) &
            (loss_df["product_name"].fillna("").str.strip() != "")
        ]
        for _, row in completed.iterrows():
            p_name = str(row["product_name"]).strip()
            m_code = str(row.get("meat_code", "")).strip()
            if p_name and m_code:
                key = (p_name, m_code)
                if key not in active_mappings:
                    active_mappings[key] = {
                        "meat_name": str(row.get("meat_name", "")).strip(),
                        "origin_grade": str(row.get("origin_grade", "")).strip(),
                    }

    # Source 2: production_status_items (신규 생산현황 데이터)
    try:
        # 그룹별로 상품-원육 매핑 추출
        groups_result = supabase.table("production_status_groups").select("id").execute()
        if groups_result.data:
            for g_row in groups_result.data:
                gid = g_row["id"]
                items_result = supabase.table("production_status_items").select("*").eq("group_id", gid).execute()
                if items_result.data:
                    items_df = pd.DataFrame(items_result.data)
                    meats = items_df[items_df["item_type"] == "raw_meat"]
                    prods = items_df[items_df["item_type"] == "product"]

                    for _, prod in prods.iterrows():
                        p_name = str(prod.get("product_name", "")).strip()
                        if not p_name:
                            continue
                        for _, meat in meats.iterrows():
                            m_code = str(meat.get("meat_code", "")).strip()
                            m_name = str(meat.get("meat_name", "")).strip()
                            m_origin = str(meat.get("meat_origin", "")).strip()
                            m_grade = str(meat.get("meat_grade", "")).strip()
                            origin_grade = f"{m_origin} {m_grade}".strip()
                            if m_code or m_name:
                                key = (p_name, m_code)
                                if key not in active_mappings:
                                    active_mappings[key] = {
                                        "meat_name": m_name,
                                        "origin_grade": origin_grade,
                                    }
    except:
        pass

    # 기존 매핑에서 더 이상 활성이 아닌 것 삭제
    load_product_rawmeats.clear()
    pr_df = load_product_rawmeats()
    if not pr_df.empty:
        for _, row in pr_df.iterrows():
            key = (str(row["product_name"]).strip(), str(row["meat_code"]).strip())
            if key not in active_mappings:
                try:
                    supabase.table("product_rawmeats").delete().eq("id", int(row["id"])).execute()
                except:
                    pass

    # 활성 매핑 upsert
    for (p_name, m_code), info in active_mappings.items():
        try:
            supabase.table("product_rawmeats").upsert(
                {
                    "product_name": p_name,
                    "meat_code": m_code,
                    "meat_name": info["meat_name"],
                    "origin_grade": info["origin_grade"],
                },
                on_conflict="product_name,meat_code"
            ).execute()
        except:
            pass

    load_product_rawmeats.clear()
