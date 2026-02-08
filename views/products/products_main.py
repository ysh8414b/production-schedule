import streamlit as st

st.title("📦 제품 관리")
st.caption("제품, 원육, 브랜드, 로스 등 제품 전반을 관리합니다.")

# ========================
# 하위 탭 네비게이션
# ========================

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📋 제품",
    "🥩 원육",
    "🏷️ 브랜드",
    "📉 로스",
    "📦 재고"
])

with tab1:
    from views.products.product_tab import render_product_tab
    render_product_tab()

with tab2:
    from views.products.rawmeat_tab import render_rawmeat_tab
    render_rawmeat_tab()

with tab3:
    from views.products.brand_tab import render_brand_tab
    render_brand_tab()

with tab4:
    from views.products.loss_tab import render_loss_tab
    render_loss_tab()

with tab5:
    from views.products.inventory_tab import render_inventory_tab
    render_inventory_tab()
