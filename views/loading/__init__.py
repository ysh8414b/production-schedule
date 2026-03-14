import streamlit as st
import pandas as pd
from utils.auth import get_supabase_client, is_authenticated

supabase = get_supabase_client()

@st.cache_data(ttl=120)
def load_loading_products():
    result = supabase.table("loading_products").select("*").order("product_code").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame(columns=[
        "id", "product_code", "product_name", "image_product_name",
        "qty_per_box", "box_height", "production_site", "loading_method", "display_color"
    ])

def upsert_loading_product(product_code, product_name, image_product_name,
                           qty_per_box, box_height, production_site, loading_method, display_color):
    client = get_supabase_client()
    client.table("loading_products").upsert(
        {
            "product_code": str(product_code).strip(),
            "product_name": str(product_name).strip(),
            "image_product_name": str(image_product_name).strip() if image_product_name else "",
            "qty_per_box": int(qty_per_box or 1),
            "box_height": int(box_height or 0),
            "production_site": str(production_site).strip() if production_site else "",
            "loading_method": str(loading_method).strip() if loading_method else "",
            "display_color": str(display_color).strip() if display_color else "#CCCCCC",
        },
        on_conflict="product_code"
    ).execute()
    load_loading_products.clear()

def upsert_loading_products_bulk(rows):
    for row in rows:
        upsert_loading_product(**row)

def delete_loading_product(product_id):
    client = get_supabase_client()
    client.table("loading_products").delete().eq("id", product_id).execute()
    load_loading_products.clear()
