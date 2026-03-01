import streamlit as st

st.title("📊 판매 데이터")
st.caption("제품 판매량 데이터를 관리합니다.")

from views.sales.product_sales_tab import render_product_sales_tab
render_product_sales_tab()
