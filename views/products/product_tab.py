import streamlit as st
import pandas as pd
from io import BytesIO
from views.products import (
    load_products, upsert_product, upsert_products_bulk,
    delete_product, update_product_fields, show_editable_table,
    update_product_by_id
)
from views.products.rawmeat_tab import load_raw_meats


def render_product_tab():
    """제품 관리 탭: 목록/등록/수정/엑셀 업로드/다운로드"""

    menu = st.radio("선택", [
        "📋 제품 목록",
        "✏️ 제품 등록/수정",
        "📥 엑셀 다운로드"
    ], horizontal=True, key="product_menu")

    st.divider()

    if menu == "📋 제품 목록":
        _show_product_list()
    elif menu == "✏️ 제품 등록/수정":
        _show_product_form()
    elif menu == "📥 엑셀 다운로드":
        _show_excel_download()


def _show_product_list():
    st.subheader("등록된 제품 목록")
    st.caption("💡 분류, 생산시간, 생산시점, 최소생산수량 셀을 직접 클릭하여 수정할 수 있습니다. 사용원육은 '제품 등록/수정'에서 변경하세요.")

    df = load_products()

    if df.empty:
        st.info("등록된 제품이 없습니다. '제품 등록/수정' 또는 '엑셀 업로드'로 추가해주세요.")
        return

    # 필터 모드
    filter_mode = st.radio(
        "🔎 보기 방식",
        ["전체 보기", "분류별 보기", "사용원육별 보기", "🔍 검색"],
        horizontal=True,
        key="prod_filter_mode"
    )

    st.divider()

    filtered_df = df.copy()

    if filter_mode == "🔍 검색":
        search = st.text_input("🔍 제품 검색", placeholder="제품코드 또는 제품명 입력...", key="prod_search")
        if search:
            mask = (
                filtered_df["product_name"].astype(str).str.contains(search, case=False, na=False) |
                filtered_df["product_code"].astype(str).str.contains(search, case=False, na=False)
            )
            filtered_df = filtered_df[mask]

    elif filter_mode == "분류별 보기":
        categories = df["category"].fillna("").astype(str).str.strip()
        unique_cats = sorted(categories[categories != ""].unique().tolist())

        if not unique_cats:
            st.warning("분류가 등록된 제품이 없습니다.")
        else:
            all_cats = ["전체"] + unique_cats
            selected_cat = st.selectbox("📂 분류 선택", options=all_cats, index=0, key="prod_cat_filter")

            if selected_cat != "전체":
                filtered_df = filtered_df[
                    filtered_df["category"].fillna("").astype(str).str.strip() == selected_cat
                ]
                st.info(f"📂 **{selected_cat}** — {len(filtered_df)}개 제품")

    elif filter_mode == "사용원육별 보기":
        meats = df["used_raw_meat"].fillna("").astype(str).str.strip()
        unique_meats = sorted(meats[meats != ""].unique().tolist())

        if not unique_meats:
            st.warning("사용원육이 등록된 제품이 없습니다.")
        else:
            all_meats = ["전체"] + unique_meats
            selected_meat = st.selectbox("🥩 사용원육 선택", options=all_meats, index=0, key="prod_meat_filter")

            if selected_meat != "전체":
                filtered_df = filtered_df[
                    filtered_df["used_raw_meat"].fillna("").astype(str).str.strip() == selected_meat
                ]
                st.info(f"🥩 **{selected_meat}** — {len(filtered_df)}개 제품")

    # 메트릭
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("표시 제품 수", f"{len(filtered_df)}개")
    with col2:
        if "category" in filtered_df.columns:
            cats = filtered_df["category"].fillna("").astype(str).str.strip()
            st.metric("분류 수", f"{cats[cats != ''].nunique()}개")
    with col3:
        if "used_raw_meat" in filtered_df.columns:
            meats_col = filtered_df["used_raw_meat"].fillna("").astype(str).str.strip()
            st.metric("원육 종류", f"{meats_col[meats_col != ''].nunique()}개")

    st.divider()

    # 제품 테이블
    if filtered_df.empty:
        st.info("조건에 맞는 제품이 없습니다.")
    else:
        if filter_mode in ["분류별 보기", "사용원육별 보기"]:
            group_col = "category" if filter_mode == "분류별 보기" else "used_raw_meat"

            selected_value = None
            if filter_mode == "분류별 보기":
                selected_value = st.session_state.get("prod_cat_filter", "전체")
            else:
                selected_value = st.session_state.get("prod_meat_filter", "전체")

            if selected_value != "전체":
                show_editable_table(filtered_df, f"prod_editor_{filter_mode}_{selected_value}")
            else:
                groups = filtered_df[group_col].fillna("").astype(str).str.strip()
                filtered_df = filtered_df.copy()
                filtered_df["_group"] = groups
                unique_groups = sorted(filtered_df["_group"].unique().tolist())

                for grp in unique_groups:
                    grp_label = grp if grp else "(미분류)"
                    grp_df = filtered_df[filtered_df["_group"] == grp]
                    with st.expander(f"📂 {grp_label}  ({len(grp_df)}개)", expanded=False):
                        show_editable_table(grp_df, f"prod_editor_grp_{grp_label}")
        else:
            show_editable_table(filtered_df, "prod_editor_main")

    st.divider()
    st.subheader("🗑️ 제품 삭제")

    delete_options = filtered_df.apply(lambda r: f"{r['product_code']} - {r['product_name']}", axis=1).tolist()
    delete_targets = st.multiselect(
        "삭제할 제품 선택 (다중 선택 가능)", options=delete_options,
        placeholder="제품을 선택하세요...", key="prod_delete_targets"
    )

    if delete_targets:
        st.warning(f"⚠️ 선택된 {len(delete_targets)}개 제품이 삭제됩니다.")
        col_a, col_b = st.columns([1, 4])
        with col_a:
            if st.button(f"🗑️ {len(delete_targets)}개 삭제", type="primary", key="prod_delete_btn"):
                deleted = 0
                for target in delete_targets:
                    try:
                        p_code = target.split(" - ")[0]
                        match = df[df["product_code"] == p_code]
                        if not match.empty:
                            delete_product(match.iloc[0]["id"])
                            deleted += 1
                    except Exception as e:
                        st.error(f"❌ '{target}' 삭제 실패: {str(e)}")
                if deleted > 0:
                    st.success(f"✅ {deleted}개 제품 삭제 완료!")
                    st.rerun()


def _show_product_form():
    st.subheader("제품 등록 / 수정")
    st.caption("이미 존재하는 제품코드를 입력하면 자동으로 수정됩니다.")

    # 성공 메시지 표시
    if st.session_state.get("product_save_msg"):
        st.success(st.session_state["product_save_msg"])
        del st.session_state["product_save_msg"]

    # 폼 리셋 처리 (카운터 증가로 폼 키를 변경하여 완전 초기화)
    if st.session_state.get("product_form_reset"):
        st.session_state["product_form_counter"] = st.session_state.get("product_form_counter", 0) + 1
        # 기존 제품 선택도 초기화
        if "prod_existing_select" in st.session_state:
            del st.session_state["prod_existing_select"]
        del st.session_state["product_form_reset"]

    form_counter = st.session_state.get("product_form_counter", 0)

    df = load_products()

    # 원육 원산지 매핑
    raw_meats_df_for_select = load_raw_meats()
    meat_origin_map = {}
    if not raw_meats_df_for_select.empty:
        for _, r in raw_meats_df_for_select.iterrows():
            name = str(r.get("name", "")).strip()
            origin = str(r.get("origin", "")).strip() if r.get("origin") else ""
            if name and origin:
                meat_origin_map[name] = origin

    def _build_option_label(r):
        code = str(r["product_code"]).strip()
        name = str(r["product_name"]).strip()
        meat = str(r.get("used_raw_meat", "")).strip() if r.get("used_raw_meat") else ""
        # 원육명/원산지 분리
        if meat and " (" in meat and meat.endswith(")"):
            meat_name = meat.rsplit(" (", 1)[0]
            meat_origin = meat.rsplit(" (", 1)[1].rstrip(")")
        else:
            meat_name = meat
            meat_origin = ""
        point = str(r.get("production_point", "")).strip() if r.get("production_point") else ""
        label = f"{code} | {name}"
        if meat_name:
            label += f" - {meat_name}"
            if meat_origin:
                label += f" {meat_origin}"
        if point:
            label += f" ({point})"
        return label

    existing_options = [""] + df.apply(_build_option_label, axis=1).tolist()
    existing = st.selectbox(
        "기존 제품 수정 (새 제품이면 비워두세요)",
        options=existing_options, index=0, key="prod_existing_select"
    )

    if existing:
        product_code = existing.split(" | ")[0].strip()
        row = df[df["product_code"] == product_code].iloc[0]
        default_id = row["id"]
        default_code = row["product_code"]
        default_name = row["product_name"]
        default_meat = row.get("used_raw_meat", "") or ""
        default_cat = row.get("category", "") or ""
        default_prod_time = int(row.get("production_time_per_unit", 0) or 0)
        default_prod_point = row.get("production_point", "") or ""
        default_min_qty = int(row.get("minimum_production_quantity", 0) or 0)
    else:
        default_id = None
        default_code = ""
        default_name = ""
        default_meat = ""
        default_cat = ""
        default_prod_time = 0
        default_prod_point = ""
        default_min_qty = 0

    # 원육 목록 로드 (원육명 + 원산지 표시)
    raw_meats_df = load_raw_meats()
    if not raw_meats_df.empty:
        meat_labels = []
        for _, r in raw_meats_df.iterrows():
            name = str(r.get("name", "")).strip()
            origin = str(r.get("origin", "")).strip() if r.get("origin") else ""
            if name:
                label = f"{name} ({origin})" if origin else name
                meat_labels.append(label)
        meat_labels = sorted(set(meat_labels))
    else:
        meat_labels = []
    meat_options = [""] + meat_labels

    # 기존 값 매칭 (원육명만 저장되어 있으므로 라벨에서 찾기)
    def find_meat_index(default_val, options):
        if not default_val:
            return 0
        default_val = str(default_val).strip()
        for i, opt in enumerate(options):
            if opt == default_val:
                return i
        # 하위호환: 기존 데이터에 원육명만 있는 경우 (원산지 없이 저장된 경우)
        for i, opt in enumerate(options):
            if opt.startswith(default_val + " (") or opt == default_val.split(" (")[0]:
                return i
        return 0

    # 선택 + 카운터 기반 폼 키 (선택 변경 또는 저장 시 폼 완전 재생성)
    form_id = f"product_form_{form_counter}_{existing or 'new'}"

    with st.form(form_id):
        product_code = st.text_input("제품코드", value=default_code)
        product_name = st.text_input("제품명", value=default_name)

        col1, col2 = st.columns(2)
        with col1:
            if meat_options:
                meat_idx = find_meat_index(default_meat, meat_options)
                used_raw_meat_label = st.selectbox(
                    "사용원육", options=meat_options, index=meat_idx
                )
                # 원육명+원산지 그대로 저장 (예: "소목심 (호주)")
                used_raw_meat = used_raw_meat_label if used_raw_meat_label else ""
            else:
                used_raw_meat = st.text_input("사용원육", value=default_meat, placeholder="예: 등심, 안심")
        with col2:
            category = st.text_input("분류", value=default_cat, placeholder="예: 정육, 가공")

        st.divider()
        st.caption("⏱️ 생산 정보")
        col3, col4, col5 = st.columns(3)
        with col3:
            prod_time_str = st.text_input("개당 생산시간(초)", value=str(default_prod_time) if default_prod_time else "")
        with col4:
            production_point = st.selectbox("생산시점", options=["주야", "주", "야"], index=["주야", "주", "야"].index(default_prod_point) if default_prod_point in ["주야", "주", "야"] else 0)
        with col5:
            min_qty_str = st.text_input("최소 생산 수량", value=str(default_min_qty) if default_min_qty else "")

        submitted = st.form_submit_button("💾 저장", type="primary")

        if submitted:
            if not product_code.strip():
                st.error("제품코드를 입력해주세요.")
            elif not product_name.strip():
                st.error("제품명을 입력해주세요.")
            else:
                try:
                    production_time_per_unit = int(prod_time_str) if prod_time_str.strip() else 0
                except ValueError:
                    production_time_per_unit = 0
                try:
                    minimum_production_quantity = int(min_qty_str) if min_qty_str.strip() else 0
                except ValueError:
                    minimum_production_quantity = 0
                if default_id is not None:
                    update_product_by_id(default_id, product_code.strip(), product_name.strip(), used_raw_meat, category,
                                         production_time_per_unit, production_point, minimum_production_quantity)
                else:
                    upsert_product(product_code.strip(), product_name.strip(), used_raw_meat, category,
                                   production_time_per_unit, production_point, minimum_production_quantity)
                st.session_state["product_save_msg"] = f"✅ '{product_name}' 저장 완료!"
                st.session_state["product_form_reset"] = True
                st.toast(f"✅ '{product_name}' 저장 완료!")
                st.rerun()


def _show_excel_download():
    st.subheader("제품 목록 다운로드")

    df = load_products()

    if df.empty:
        st.info("등록된 제품이 없습니다.")
        return

    st.caption(f"총 {len(df)}개 제품")

    display_cols = ["product_code", "product_name", "used_raw_meat", "category",
                    "production_time_per_unit", "production_point", "minimum_production_quantity"]
    display_cols = [c for c in display_cols if c in df.columns]
    display_df = df[display_cols].copy()
    
    # 사용원육에서 원육명/원산지 분리
    def _extract_meat_name(val):
        val = str(val).strip()
        if " (" in val and val.endswith(")"):
            return val.rsplit(" (", 1)[0]
        return val
    
    def _extract_origin(val):
        val = str(val).strip()
        if " (" in val and val.endswith(")"):
            return val.rsplit(" (", 1)[1].rstrip(")")
        return ""
    
    meat_idx = display_df.columns.get_loc("used_raw_meat") + 1
    display_df.insert(meat_idx, "origin", display_df["used_raw_meat"].fillna("").apply(_extract_origin))
    display_df["used_raw_meat"] = display_df["used_raw_meat"].fillna("").apply(_extract_meat_name)
    
    display_df = display_df.rename(columns={
        "product_code": "제품코드", "product_name": "제품명",
        "used_raw_meat": "사용원육", "origin": "원산지", "category": "분류",
        "production_time_per_unit": "개당 생산시간(초)",
        "production_point": "생산시점",
        "minimum_production_quantity": "최소생산수량"
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        display_df.to_excel(writer, index=False, sheet_name="제품목록")

    st.download_button(
        label="💾 Excel 다운로드",
        data=output.getvalue(),
        file_name="제품목록.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="prod_download_btn"
    )
