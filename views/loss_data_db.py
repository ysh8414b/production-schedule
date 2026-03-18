"""
로스 데이터 DB 함수 모듈.
loss_data.py에서 렌더링 코드와 분리하여,
다른 모듈(product_info.py 등)에서 import 시 UI가 렌더링되지 않도록 함.
"""
import streamlit as st
import pandas as pd
from utils.auth import get_supabase_client

supabase = get_supabase_client()


@st.cache_data(ttl=120)
def load_production_status_uploads():
    """업로드 배치 목록 조회"""
    try:
        result = supabase.table("production_status_uploads").select("*").order("upload_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except Exception:
        st.toast("업로드 목록 조회 실패", icon="⚠️")
    return pd.DataFrame(columns=["id", "upload_date", "file_name", "total_groups",
                                  "total_input_kg", "total_output_kg", "total_loss_kg"])


@st.cache_data(ttl=120)
def load_production_status_groups(upload_id=None):
    """그룹 목록 조회"""
    try:
        query = supabase.table("production_status_groups").select("*").order("group_index")
        if upload_id:
            query = query.eq("upload_id", upload_id)
        result = query.execute()
        if result.data:
            return pd.DataFrame(result.data)
    except Exception:
        st.toast("그룹 목록 조회 실패", icon="⚠️")
    return pd.DataFrame(columns=["id", "upload_id", "group_index", "total_input_kg",
                                  "total_output_kg", "loss_kg", "loss_rate",
                                  "total_input_amount", "total_output_amount"])


@st.cache_data(ttl=120)
def load_production_status_items(group_id=None):
    """항목 목록 조회"""
    try:
        query = supabase.table("production_status_items").select("*").order("id")
        if group_id:
            query = query.eq("group_id", group_id)
        result = query.execute()
        if result.data:
            return pd.DataFrame(result.data)
    except Exception:
        st.toast("항목 조회 실패", icon="⚠️")
    return pd.DataFrame()


@st.cache_data(ttl=120)
def load_production_status_items_bulk(group_ids):
    """여러 그룹의 항목을 한 번에 조회 (N+1 쿼리 방지)"""
    if not group_ids:
        return pd.DataFrame()
    try:
        # Supabase .in_() 필터는 대량 ID에 제한이 있을 수 있으므로 청크 처리
        all_data = []
        chunk_size = 200
        for i in range(0, len(group_ids), chunk_size):
            chunk = list(group_ids[i:i + chunk_size])
            result = supabase.table("production_status_items").select("*").in_("group_id", chunk).order("id").execute()
            if result.data:
                all_data.extend(result.data)
        if all_data:
            return pd.DataFrame(all_data)
    except Exception:
        st.toast("항목 일괄 조회 실패", icon="⚠️")
    return pd.DataFrame()


def clear_production_status_caches():
    """캐시 클리어"""
    load_production_status_uploads.clear()
    load_production_status_groups.clear()
    load_production_status_items.clear()
    load_production_status_items_bulk.clear()


def insert_production_status(upload_data, groups_with_items):
    """
    생산현황 데이터 일괄 저장 (배치 최적화).
    upload_data: dict (upload_date, file_name, total_groups, total_input_kg, total_output_kg, total_loss_kg)
    groups_with_items: list of dict, each with:
        group_data: dict (group_index, total_input_kg, total_output_kg, loss_kg, loss_rate, ...)
        items: list of dict (item rows)
    """
    client = get_supabase_client()

    # 1. 업로드 배치 생성
    upload_result = client.table("production_status_uploads").insert(upload_data).execute()
    upload_id = upload_result.data[0]["id"]

    try:
        # 2. 그룹 배치 INSERT (한 번에 모든 그룹 저장)
        group_rows = []
        for group_info in groups_with_items:
            group_data = group_info["group_data"].copy()
            group_data["upload_id"] = upload_id
            group_rows.append(group_data)

        if group_rows:
            # 500건씩 배치 INSERT
            chunk_size = 500
            all_group_results = []
            for i in range(0, len(group_rows), chunk_size):
                chunk = group_rows[i:i + chunk_size]
                result = client.table("production_status_groups").insert(chunk).execute()
                all_group_results.extend(result.data)

            # 3. group_index → group_id 매핑 생성
            group_id_map = {}
            for g in all_group_results:
                group_id_map[g["group_index"]] = g["id"]

            # 4. 모든 items에 group_id 할당 후 배치 INSERT
            all_items = []
            for group_info in groups_with_items:
                group_index = group_info["group_data"]["group_index"]
                group_id = group_id_map.get(group_index)
                if group_id and group_info["items"]:
                    for item in group_info["items"]:
                        item["group_id"] = group_id
                    all_items.extend(group_info["items"])

            if all_items:
                for i in range(0, len(all_items), chunk_size):
                    chunk = all_items[i:i + chunk_size]
                    client.table("production_status_items").insert(chunk).execute()

    except Exception as e:
        # 실패 시 이미 생성된 upload 배치 삭제 (CASCADE로 하위 데이터도 정리)
        try:
            client.table("production_status_uploads").delete().eq("id", upload_id).execute()
        except Exception:
            pass
        raise e

    clear_production_status_caches()
    return upload_id


def delete_production_status_upload(upload_id):
    """업로드 배치 삭제 (CASCADE로 groups, items 자동 삭제)"""
    client = get_supabase_client()
    client.table("production_status_uploads").delete().eq("id", upload_id).execute()
    clear_production_status_caches()
