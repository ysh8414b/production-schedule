import streamlit as st

st.title("📋 적재리스트")
st.caption("발주서 기반 파렛트 적재리스트를 자동 생성합니다.")

tab1, tab2, tab3 = st.tabs([
    "📤 발주서 업로드",
    "📦 발주 제품 정보",
    "📊 생성된 적재리스트"
])

with tab1:
    from views.loading.upload_tab import render_upload_tab
    render_upload_tab()

with tab2:
    from views.loading.product_info_tab import render_product_info_tab
    render_product_info_tab()

with tab3:
    from views.loading.result_tab import render_result_tab
    render_result_tab()
