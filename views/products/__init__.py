import streamlit as st
import pandas as pd
from io import BytesIO
from utils.auth import get_supabase_client, is_authenticated, can_edit

# ========================
# Supabase 연결
# ========================

supabase = get_supabase_client()


def _clear_schedule_caches():
    """스케줄 페이지의 캐시도 함께 클리어 (제품/재고 변경 시 즉시 반영)"""
    try:
        from views.schedule import load_inventory_from_db, load_all_product_names
        load_inventory_from_db.clear()
        load_all_product_names.clear()
    except Exception:
        pass


# ========================
# 공통 DB 함수
# ========================

@st.cache_data(ttl=120)
def load_products():
    """제품 목록 로드 (캐시 2분)"""
    result = supabase.table("products").select("*").order("product_name").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame(columns=["id", "product_code", "product_name", "used_raw_meat", "category", "current_stock"])

def upsert_product(code, name, used_raw_meat, category,
                   production_time_per_unit=0, production_point="", minimum_production_quantity=0):
    client = get_supabase_client()
    client.table("products").upsert(
        {
            "product_code": str(code).strip(),
            "product_name": str(name).strip(),
            "used_raw_meat": str(used_raw_meat).strip() if used_raw_meat else "",
            "category": str(category).strip() if category else "",
            "production_time_per_unit": int(production_time_per_unit or 0),
            "production_point": str(production_point).strip() if production_point else "",
            "minimum_production_quantity": int(minimum_production_quantity or 0),
        },
        on_conflict="product_code"
    ).execute()
    load_products.clear()
    _clear_schedule_caches()

def upsert_products_bulk(rows):
    client = get_supabase_client()
    client.table("products").upsert(
        rows,
        on_conflict="product_code"
    ).execute()
    load_products.clear()
    _clear_schedule_caches()

def update_product_by_id(product_id, code, name, used_raw_meat, category,
                         production_time_per_unit=0, production_point="", minimum_production_quantity=0):
    """기존 제품을 ID 기준으로 업데이트 (제품코드 변경도 가능)"""
    client = get_supabase_client()
    client.table("products").update(
        {
            "product_code": str(code).strip(),
            "product_name": str(name).strip(),
            "used_raw_meat": str(used_raw_meat).strip() if used_raw_meat else "",
            "category": str(category).strip() if category else "",
            "production_time_per_unit": int(production_time_per_unit or 0),
            "production_point": str(production_point).strip() if production_point else "",
            "minimum_production_quantity": int(minimum_production_quantity or 0),
        }
    ).eq("id", product_id).execute()
    load_products.clear()
    _clear_schedule_caches()


def delete_product(product_id):
    client = get_supabase_client()
    client.table("products").delete().eq("id", product_id).execute()
    load_products.clear()
    _clear_schedule_caches()

def update_product_fields(product_code, used_raw_meat, category,
                          production_time_per_unit=None, production_point=None, minimum_production_quantity=None):
    """사용원육, 분류, 생산정보 업데이트"""
    updates = {
        "used_raw_meat": str(used_raw_meat).strip() if pd.notna(used_raw_meat) else "",
        "category": str(category).strip() if pd.notna(category) else "",
    }
    if production_time_per_unit is not None:
        updates["production_time_per_unit"] = int(production_time_per_unit) if pd.notna(production_time_per_unit) else 0
    if production_point is not None:
        updates["production_point"] = str(production_point).strip() if pd.notna(production_point) else ""
    if minimum_production_quantity is not None:
        updates["minimum_production_quantity"] = int(minimum_production_quantity) if pd.notna(minimum_production_quantity) else 0

    client = get_supabase_client()
    client.table("products").update(updates).eq("product_code", product_code).execute()
    load_products.clear()
    _clear_schedule_caches()

def update_product_stock(product_code, current_stock):
    """현 재고 업데이트"""
    client = get_supabase_client()
    client.table("products").update(
        {"current_stock": int(current_stock)}
    ).eq("product_code", product_code).execute()
    load_products.clear()
    _clear_schedule_caches()

def update_product_stocks_bulk(updates):
    """여러 제품 재고 일괄 업데이트. updates: list of dict with product_code, current_stock"""
    if not updates:
        return
    client = get_supabase_client()
    # upsert 배치로 한 번에 업데이트 (개별 UPDATE 대신)
    chunk_size = 500
    upsert_rows = [
        {"product_code": item["product_code"], "current_stock": int(item["current_stock"])}
        for item in updates
    ]
    for i in range(0, len(upsert_rows), chunk_size):
        client.table("products").upsert(
            upsert_rows[i:i + chunk_size], on_conflict="product_code"
        ).execute()
    load_products.clear()
    _clear_schedule_caches()

def _get_meat_origin_map():
    """원육명 → 원산지 매핑 (raw_meats 테이블에서)"""
    try:
        result = supabase.table("raw_meats").select("name, origin").execute()
        if result.data:
            meat_map = {}
            for row in result.data:
                name = str(row.get("name", "")).strip()
                origin = str(row.get("origin", "")).strip() if row.get("origin") else ""
                if name and origin:
                    meat_map[name] = origin
            return meat_map
    except:
        pass
    return {}


def _get_meat_select_options():
    """원육 선택 옵션 목록 (원육명 + 원산지)"""
    try:
        result = supabase.table("raw_meats").select("name, origin").execute()
        if result.data:
            options = []
            for row in result.data:
                name = str(row.get("name", "")).strip()
                origin = str(row.get("origin", "")).strip() if row.get("origin") else ""
                if name:
                    label = f"{name} ({origin})" if origin else name
                    options.append(label)
            return sorted(set(options))
    except:
        pass
    return []


def show_editable_table(filtered_df, editor_key):
    """사용원육/분류/생산정보를 인라인 수정 가능한 data_editor"""
    authenticated = can_edit("products")
    cols = ["product_code", "product_name", "used_raw_meat", "category"]
    # 생산정보 컬럼이 있으면 추가
    for c in ["production_time_per_unit", "production_point", "minimum_production_quantity"]:
        if c in filtered_df.columns:
            cols.append(c)
    
    edit_df = filtered_df[cols].copy()

    # 원산지 컬럼 추가 (used_raw_meat에서 파싱, 하위호환용 fallback)
    meat_origin_map = _get_meat_origin_map()
    
    def _extract_origin(val):
        val = str(val).strip()
        if " (" in val and val.endswith(")"):
            return val.rsplit(" (", 1)[1].rstrip(")")
        return meat_origin_map.get(val, "")
    
    def _extract_meat_name(val):
        val = str(val).strip()
        if " (" in val and val.endswith(")"):
            return val.rsplit(" (", 1)[0]
        return val
    
    meat_col_idx = edit_df.columns.get_loc("used_raw_meat") + 1
    edit_df.insert(meat_col_idx, "origin", edit_df["used_raw_meat"].fillna("").apply(_extract_origin))
    
    # 사용원육은 원육명만 표시 (원산지는 별도 컬럼)
    edit_df["used_raw_meat"] = edit_df["used_raw_meat"].fillna("").apply(_extract_meat_name)
    
    # NaN 처리
    if "production_time_per_unit" in edit_df.columns:
        edit_df["production_time_per_unit"] = edit_df["production_time_per_unit"].fillna(0).astype(int)
    if "production_point" in edit_df.columns:
        edit_df["production_point"] = edit_df["production_point"].fillna("").astype(str)
    if "minimum_production_quantity" in edit_df.columns:
        edit_df["minimum_production_quantity"] = edit_df["minimum_production_quantity"].fillna(0).astype(int)
    
    rename_map = {
        "product_code": "제품코드",
        "product_name": "제품명",
        "used_raw_meat": "사용원육",
        "origin": "원산지",
        "category": "분류",
        "production_time_per_unit": "개당 생산시간(초)",
        "production_point": "생산시점",
        "minimum_production_quantity": "최소생산수량",
    }
    edit_df = edit_df.rename(columns=rename_map)

    col_config = {
        "제품코드": st.column_config.TextColumn("제품코드", width="medium"),
        "제품명": st.column_config.TextColumn("제품명", width="large"),
        "사용원육": st.column_config.TextColumn("사용원육", width="medium"),
        "원산지": st.column_config.TextColumn("원산지", width="small"),
        "분류": st.column_config.TextColumn("분류", width="medium"),
    }
    if "개당 생산시간(초)" in edit_df.columns:
        col_config["개당 생산시간(초)"] = st.column_config.NumberColumn("생산시간(초)", width="small", min_value=0, step=1)
    if "생산시점" in edit_df.columns:
        col_config["생산시점"] = st.column_config.SelectboxColumn("생산시점", width="small", options=["주야", "주", "야"])
    if "최소생산수량" in edit_df.columns:
        col_config["최소생산수량"] = st.column_config.NumberColumn("최소생산수량", width="small", min_value=0, step=1)

    disabled_cols = ["제품코드", "제품명", "사용원육", "원산지"]
    if not authenticated:
        disabled_cols = True  # 모든 컬럼 비활성화

    edited = st.data_editor(
        edit_df,
        use_container_width=True,
        hide_index=True,
        key=editor_key,
        disabled=disabled_cols,
        column_config=col_config
    )

    original = edit_df.reset_index(drop=True)
    changed = edited.reset_index(drop=True)

    # 변경 감지 — 기존 컬럼 + 새 컬럼
    diff_mask = (original["사용원육"] != changed["사용원육"]) | (original["분류"] != changed["분류"])
    if "개당 생산시간(초)" in original.columns:
        diff_mask = diff_mask | (original["개당 생산시간(초)"] != changed["개당 생산시간(초)"])
    if "생산시점" in original.columns:
        diff_mask = diff_mask | (original["생산시점"].astype(str) != changed["생산시점"].astype(str))
    if "최소생산수량" in original.columns:
        diff_mask = diff_mask | (original["최소생산수량"] != changed["최소생산수량"])
    
    changed_rows = changed[diff_mask]

    if len(changed_rows) > 0 and authenticated:
        st.info(f"✏️ **{len(changed_rows)}개** 제품이 수정되었습니다. 아래 버튼을 눌러 저장하세요.")
        if st.button("💾 변경사항 저장", type="primary", key=f"save_{editor_key}"):
            for _, row in changed_rows.iterrows():
                update_product_fields(
                    row["제품코드"],
                    row["사용원육"],
                    row["분류"],
                    production_time_per_unit=row.get("개당 생산시간(초)"),
                    production_point=row.get("생산시점"),
                    minimum_production_quantity=row.get("최소생산수량"),
                )
            st.success(f"✅ {len(changed_rows)}개 제품 수정 완료!")
            st.rerun()
