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
    except:
        pass
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
    except:
        pass
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
    except:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=120)
def load_production_status_items_bulk(group_ids):
    """여러 그룹의 항목을 한 번에 조회 (N+1 쿼리 방지)"""
    if not group_ids:
        return pd.DataFrame()
    try:
        result = supabase.table("production_status_items").select("*").in_("group_id", list(group_ids)).order("id").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame()


def clear_production_status_caches():
    """캐시 클리어"""
    load_production_status_uploads.clear()
    load_production_status_groups.clear()
    load_production_status_items.clear()
    load_production_status_items_bulk.clear()


def insert_production_status(upload_data, groups_with_items):
    """
    생산현황 데이터 일괄 저장.
    upload_data: dict (upload_date, file_name, total_groups, total_input_kg, total_output_kg, total_loss_kg)
    groups_with_items: list of dict, each with:
        group_data: dict (group_index, total_input_kg, total_output_kg, loss_kg, loss_rate, ...)
        items: list of dict (item rows)
    """
    client = get_supabase_client()
    # 1. 업로드 배치 생성
    upload_result = client.table("production_status_uploads").insert(upload_data).execute()
    upload_id = upload_result.data[0]["id"]

    # 2. 그룹별 저장
    for group_info in groups_with_items:
        group_data = group_info["group_data"].copy()
        group_data["upload_id"] = upload_id

        group_result = client.table("production_status_groups").insert(group_data).execute()
        group_id = group_result.data[0]["id"]

        # 3. 항목 저장
        items = group_info["items"]
        if items:
            for item in items:
                item["group_id"] = group_id
            # 500건씩 나눠 저장
            chunk_size = 500
            for i in range(0, len(items), chunk_size):
                chunk = items[i:i + chunk_size]
                client.table("production_status_items").insert(chunk).execute()

    clear_production_status_caches()
    return upload_id


def delete_production_status_upload(upload_id):
    """업로드 배치 삭제 (CASCADE로 groups, items 자동 삭제)"""
    client = get_supabase_client()
    client.table("production_status_uploads").delete().eq("id", upload_id).execute()
    clear_production_status_caches()
