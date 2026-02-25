import streamlit as st

st.title("📊 판매 데이터")
st.caption("판매량 및 투입 원육 데이터를 관리합니다.")

# ========================
# 하위 탭 네비게이션
# ========================

tab1, tab2 = st.tabs([
    "📋 제품 판매",
    "🥩 투입 원육"
])

with tab1:
    from views.sales.product_sales_tab import render_product_sales_tab
    render_product_sales_tab()

with tab2:
    from views.sales.rawmeat_input_tab import render_rawmeat_input_tab
    render_rawmeat_input_tab()
