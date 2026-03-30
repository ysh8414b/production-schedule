import streamlit as st
import pandas as pd
from utils.auth import get_supabase_client, is_authenticated, can_edit

@st.cache_data(ttl=120)
def load_loading_products():
    result = get_supabase_client().table("loading_products").select("*").order("product_code").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame(columns=[
        "id", "product_code", "product_name", "image_product_name",
        "qty_per_box", "box_height", "company_name", "production_site", "loading_method", "display_color"
    ])

def upsert_loading_product(product_code, product_name, image_product_name,
                           qty_per_box, box_height, company_name, production_site, loading_method, display_color):
    client = get_supabase_client()
    client.table("loading_products").upsert(
        {
            "product_code": str(product_code).strip(),
            "product_name": str(product_name).strip(),
            "image_product_name": str(image_product_name).strip() if image_product_name else "",
            "qty_per_box": int(qty_per_box or 1),
            "box_height": int(box_height or 0),
            "company_name": str(company_name).strip() if company_name else "",
            "production_site": str(production_site).strip() if production_site else "",
            "loading_method": str(loading_method).strip() if loading_method else "",
            "display_color": str(display_color).strip() if display_color else "#CCCCCC",
        },
        on_conflict="product_code"
    ).execute()
    load_loading_products.clear()

def upsert_loading_products_bulk(rows):
    """벌크 upsert - 500건씩 배치 처리"""
    if not rows:
        return
    client = get_supabase_client()
    processed = []
    for row in rows:
        processed.append({
            "product_code": str(row.get("product_code", "")).strip(),
            "product_name": str(row.get("product_name", "")).strip(),
            "image_product_name": str(row.get("image_product_name", "")).strip() if row.get("image_product_name") else "",
            "qty_per_box": int(row.get("qty_per_box") or 1),
            "box_height": int(row.get("box_height") or 0),
            "company_name": str(row.get("company_name", "")).strip() if row.get("company_name") else "",
            "production_site": str(row.get("production_site", "")).strip() if row.get("production_site") else "",
            "loading_method": str(row.get("loading_method", "")).strip() if row.get("loading_method") else "",
            "display_color": str(row.get("display_color", "")).strip() if row.get("display_color") else "#CCCCCC",
        })
    chunk_size = 500
    for i in range(0, len(processed), chunk_size):
        chunk = processed[i:i + chunk_size]
        client.table("loading_products").upsert(chunk, on_conflict="product_code").execute()
    load_loading_products.clear()

def delete_loading_product(product_id):
    client = get_supabase_client()
    client.table("loading_products").delete().eq("id", product_id).execute()
    load_loading_products.clear()
