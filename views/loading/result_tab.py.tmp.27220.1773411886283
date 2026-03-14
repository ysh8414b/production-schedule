import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from views.loading import load_loading_products
from views.loading.loading_algorithm import allocate_pallets, get_pallet_summary
from views.loading.loading_excel import generate_loading_excel
from views.loading.loading_html import generate_loading_html


def render_result_tab():
    if "loading_order_data" not in st.session_state:
        st.info("발주서를 먼저 업로드해주세요. '발주서 업로드' 탭으로 이동하세요.")
        return

    order_data = st.session_state["loading_order_data"]
    products_df = load_loading_products()

    if products_df.empty:
        st.warning("발주 제품 정보가 등록되지 않았습니다. '발주 제품 정보' 탭에서 먼저 등록하세요.")
        return

    product_info = {}
    for _, row in products_df.iterrows():
        product_info[str(row["product_code"])] = {
            "product_name": row["product_name"],
            "image_product_name": row.get("image_product_name", ""),
            "qty_per_box": row["qty_per_box"],
            "box_height": row["box_height"],
            "loading_method": row["loading_method"],
            "display_color": row.get("display_color", "#CCCCCC"),
        }

    missing = [item["product_code"] for item in order_data["items"]
               if item["product_code"] not in product_info]
    if missing:
        st.error(f"다음 상품코드의 제품 정보가 없습니다: {', '.join(missing)}")
        st.info("'발주 제품 정보' 탭에서 해당 제품을 등록해주세요.")
        return

    # 업체명이 발주서에 없으면 제품정보 DB의 생산지점으로 대체
    if not order_data.get("supplier"):
        sites = [row.get("production_site", "") for _, row in products_df.iterrows() if row.get("production_site")]
        if sites:
            order_data["supplier"] = sites[0]

    pallets = allocate_pallets(order_data["items"], product_info)

    if not pallets:
        st.warning("적재 결과가 없습니다. 발주서 데이터를 확인해주세요.")
        return

    # --- HTML 적재리스트 미리보기 ---
    st.subheader(f"적재 결과: 총 {len(pallets)}파렛트")

    html_str = generate_loading_html(pallets, order_data)
    st.session_state["loading_html_str"] = html_str

    # Streamlit 내 HTML 미리보기
    components.html(html_str, height=700, scrolling=True)

    # --- 상세 데이터 ---
    with st.expander("상세 파렛트 데이터", expanded=False):
        summary_rows = []
        for p in pallets:
            for prod in p["products"]:
                summary_rows.append({
                    "파렛트": p["pallet_number"],
                    "타입": p["pallet_type"],
                    "상품코드": prod["product_code"],
                    "상품명": prod["product_name"],
                    "박스수": prod["box_count"],
                    "수량": prod["box_count"] * prod["qty_per_box"],
                })
        summary_df = pd.DataFrame(summary_rows)
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

    # --- 다운로드 버튼 ---
    st.divider()
    col_a, col_b = st.columns(2)

    with col_a:
        if st.button("적재리스트 Excel 생성", use_container_width=True):
            with st.spinner("Excel 생성 중..."):
                excel_buf = generate_loading_excel(pallets, order_data)
                st.session_state["loading_excel_buf"] = excel_buf
                st.session_state["loading_excel_name"] = f"적재리스트_{order_data['order_number']}.xlsx"

        if "loading_excel_buf" in st.session_state:
            st.download_button(
                "📥 Excel 다운로드",
                data=st.session_state["loading_excel_buf"],
                file_name=st.session_state["loading_excel_name"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    with col_b:
        if "loading_html_str" in st.session_state:
            st.download_button(
                "📥 적재리스트 HTML 다운로드",
                data=st.session_state["loading_html_str"].encode("utf-8"),
                file_name=f"적재리스트_{order_data['order_number']}.html",
                mime="text/html",
                use_container_width=True,
            )
