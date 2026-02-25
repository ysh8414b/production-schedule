import streamlit as st
import pandas as pd
from views.sales import load_product_rawmeats, sync_product_rawmeats

st.title("📦 제품")
st.caption("로스 데이터에서 자동 생성된 제품-원육 매핑을 확인합니다.")

# 페이지 로드 시 loss_assignments 기준으로 동기화
sync_product_rawmeats()

df = load_product_rawmeats()

if df.empty:
    st.info("등록된 데이터가 없습니다. 로스 데이터에서 제품 할당 시 자동으로 생성됩니다.")
else:
    products = sorted(df["product_name"].unique().tolist())
    meat_count = df[["meat_code", "meat_name"]].drop_duplicates().shape[0]

    # 메트릭
    col1, col2 = st.columns(2)
    with col1:
        st.metric("등록 제품 수", f"{len(products)}개")
    with col2:
        st.metric("사용 원육 종류", f"{meat_count}개")

    st.divider()

    # 검색
    search = st.text_input("🔍 제품 검색", placeholder="제품명 입력...", key="product_info_search")

    filtered_products = products
    if search:
        filtered_products = [p for p in products if search.lower() in p.lower()]

    if not filtered_products:
        st.info("검색 결과가 없습니다.")
    else:
        for product in filtered_products:
            product_meats = df[df["product_name"] == product]

            with st.expander(f"📦 {product} ({len(product_meats)}개 원육)", expanded=False):
                display_data = []
                for _, row in product_meats.iterrows():
                    display_data.append({
                        "원육코드": row.get("meat_code", ""),
                        "원육명": row.get("meat_name", ""),
                        "원산지(등급)": row.get("origin_grade", ""),
                    })

                display_df = pd.DataFrame(display_data)
                st.dataframe(display_df, use_container_width=True, hide_index=True)
