import streamlit as st
import pandas as pd
from views.products import load_products, update_product_stocks_bulk


def render_inventory_tab():
    """재고 관리 탭: 제품별 현재고 조회 및 수정"""

    st.subheader("📦 재고 관리")
    st.caption("💡 '현 재고' 셀을 직접 클릭하여 수정한 뒤 저장 버튼을 누르세요.")

    df = load_products()

    if df.empty:
        st.info("등록된 제품이 없습니다. '제품' 탭에서 먼저 제품을 등록해주세요.")
        return

    # current_stock 컬럼이 없으면 0으로 초기화
    if "current_stock" not in df.columns:
        df["current_stock"] = 0
    df["current_stock"] = df["current_stock"].fillna(0).astype(int)

    # ── 필터 ──
    filter_mode = st.radio(
        "🔎 보기 방식",
        ["전체 보기", "분류별 보기", "🔍 검색"],
        horizontal=True,
        key="inv_filter_mode"
    )

    filtered_df = df.copy()

    if filter_mode == "🔍 검색":
        search = st.text_input("🔍 검색", placeholder="제품코드 또는 제품명 입력...", key="inv_search")
        if search:
            mask = (
                filtered_df["product_name"].astype(str).str.contains(search, case=False, na=False) |
                filtered_df["product_code"].astype(str).str.contains(search, case=False, na=False)
            )
            filtered_df = filtered_df[mask]

    elif filter_mode == "분류별 보기":
        categories = df["category"].fillna("").astype(str).str.strip()
        unique_cats = sorted(categories[categories != ""].unique().tolist())
        if unique_cats:
            all_cats = ["전체"] + unique_cats
            selected_cat = st.selectbox("📂 분류 선택", options=all_cats, index=0, key="inv_cat_filter")
            if selected_cat != "전체":
                filtered_df = filtered_df[
                    filtered_df["category"].fillna("").astype(str).str.strip() == selected_cat
                ]

    st.divider()

    # ── 메트릭 ──
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("제품 수", f"{len(filtered_df)}개")
    with col2:
        st.metric("총 재고", f"{filtered_df['current_stock'].sum():,}개")
    with col3:
        zero_stock = (filtered_df["current_stock"] == 0).sum()
        st.metric("재고 없음", f"{zero_stock}개")

    st.divider()

    if filtered_df.empty:
        st.info("조건에 맞는 제품이 없습니다.")
        return

    filtered_df = filtered_df.sort_values("id").reset_index(drop=True)

    # ── 편집 가능한 테이블 ──
    edit_df = filtered_df[["product_code", "product_name", "current_stock"]].copy()
    edit_df["current_stock"] = edit_df["current_stock"].fillna(0).astype(int)
    edit_df = edit_df.rename(columns={
        "product_code": "제품코드",
        "product_name": "제품명",
        "current_stock": "현 재고"
    })

    edited = st.data_editor(
        edit_df,
        use_container_width=True,
        hide_index=True,
        key="inventory_editor",
        disabled=["제품코드", "제품명"],
        column_config={
            "제품코드": st.column_config.TextColumn("제품코드", width="medium"),
            "제품명": st.column_config.TextColumn("제품명", width="large"),
            "현 재고": st.column_config.NumberColumn(
                "현 재고",
                width="medium",
                min_value=0,
                step=1,
                format="%d"
            ),
        }
    )

    # ── 변경 감지 및 저장 ──
    original = edit_df.reset_index(drop=True)
    changed = edited.reset_index(drop=True)

    diff_mask = original["현 재고"] != changed["현 재고"]
    changed_rows = changed[diff_mask]

    if len(changed_rows) > 0:
        st.info(f"✏️ **{len(changed_rows)}개** 제품의 재고가 수정되었습니다.")

        # 변경 내역 미리보기
        with st.expander("변경 내역 확인", expanded=True):
            preview = changed_rows.copy()
            preview["기존 재고"] = original.loc[diff_mask, "현 재고"].values
            preview = preview[["제품코드", "제품명", "기존 재고", "현 재고"]]
            st.dataframe(preview, use_container_width=True, hide_index=True)

        if st.button("💾 재고 저장", type="primary", key="inv_save_btn"):
            updates = []
            for _, row in changed_rows.iterrows():
                stock = row["현 재고"]
                stock = 0 if pd.isna(stock) else int(stock)
                updates.append({"product_code": row["제품코드"], "current_stock": stock})
            update_product_stocks_bulk(updates)
            st.success(f"✅ {len(updates)}개 제품 재고 저장 완료!")
            st.rerun()
