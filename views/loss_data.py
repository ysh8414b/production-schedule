import streamlit as st
import pandas as pd
from supabase import create_client
from datetime import date

# ========================
# Supabase 연결
# ========================

@st.cache_resource
def _get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = _get_supabase()

# ========================
# DB 함수
# ========================

@st.cache_data(ttl=120)
def load_products():
    result = supabase.table("products").select("*").order("product_name").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame(columns=["id", "product_code", "product_name", "used_raw_meat", "category"])


@st.cache_data(ttl=60)
def load_raw_meat_inputs():
    """raw_meat_inputs 테이블에서 투입 원육 로드"""
    try:
        result = supabase.table("raw_meat_inputs").select("*").order("move_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "move_date", "meat_code", "meat_name", "origin_grade",
        "kg", "tracking_number", "product_name", "production_kg", "memo", "completed"
    ])


def insert_raw_meat_inputs(rows):
    """원육 투입 데이터 일괄 등록"""
    supabase.table("raw_meat_inputs").insert(rows).execute()
    load_raw_meat_inputs.clear()


def update_raw_meat_input(row_id, data: dict):
    """원육 투입 데이터 수정"""
    supabase.table("raw_meat_inputs").update(data).eq("id", row_id).execute()
    load_raw_meat_inputs.clear()


def delete_raw_meat_input(row_id):
    """원육 투입 데이터 삭제"""
    supabase.table("raw_meat_inputs").delete().eq("id", row_id).execute()
    load_raw_meat_inputs.clear()


# ========================
# 페이지 렌더링
# ========================

st.title("📉 로스 데이터")
st.caption("원육 투입 → 제품 할당 → 로스 관리")

menu = st.radio("선택", [
    "📤 원육 업로드",
    "📋 투입 현황 / 제품 할당",
], horizontal=True, key="loss_data_menu")

st.divider()

# ========================
# 원육 업로드
# ========================

if menu == "📤 원육 업로드":
    st.subheader("📤 원육 투입 업로드")
    st.caption("엑셀/CSV 파일로 투입된 원육을 업로드합니다.")
    st.markdown("""
    **업로드 양식 (컬럼명)**
    | 이동일자 | 원육코드 | 원육명 | 원산지(등급) | Kg | 이력번호 |
    |---------|---------|-------|------------|-----|---------|
    | 2025-01-01 | M001 | 소목심 | 호주산(1등급) | 150.5 | T20250101-001 |
    """)

    uploaded_file = st.file_uploader(
        "엑셀 또는 CSV 파일 업로드",
        type=["xlsx", "xls", "csv"],
        key="rawmeat_upload"
    )

    if uploaded_file:
        try:
            if uploaded_file.name.endswith(".csv"):
                df_upload = pd.read_csv(uploaded_file)
            else:
                df_upload = pd.read_excel(uploaded_file)

            # 컬럼 매핑 (유연하게)
            col_map = {}
            for col in df_upload.columns:
                col_clean = str(col).strip().replace(" ", "")
                if "이동일자" in col_clean or "일자" in col_clean or "날짜" in col_clean or "date" in col_clean.lower():
                    col_map[col] = "move_date"
                elif "원육코드" in col_clean or "코드" in col_clean or "code" in col_clean.lower():
                    col_map[col] = "meat_code"
                elif "원육명" in col_clean or "원육" in col_clean:
                    col_map[col] = "meat_name"
                elif "원산지" in col_clean or "등급" in col_clean or "origin" in col_clean.lower():
                    col_map[col] = "origin_grade"
                elif col_clean.lower() == "kg" or "무게" in col_clean or "중량" in col_clean:
                    col_map[col] = "kg"
                elif "이력번호" in col_clean or "이력" in col_clean or "tracking" in col_clean.lower():
                    col_map[col] = "tracking_number"

            if col_map:
                df_upload = df_upload.rename(columns=col_map)

            # 필수 컬럼 확인
            required = ["move_date", "meat_name", "kg"]
            missing = [c for c in required if c not in df_upload.columns]
            if missing:
                st.error(f"필수 컬럼이 누락되었습니다: {', '.join(missing)}")
                st.info("컬럼명을 확인해주세요: 이동일자, 원육코드, 원육명, 원산지(등급), Kg, 이력번호")
            else:
                # 데이터 정리
                for col in ["meat_code", "origin_grade", "tracking_number"]:
                    if col not in df_upload.columns:
                        df_upload[col] = ""

                df_upload["move_date"] = pd.to_datetime(df_upload["move_date"], errors="coerce").dt.strftime("%Y-%m-%d")
                df_upload["kg"] = pd.to_numeric(df_upload["kg"], errors="coerce").fillna(0)
                df_upload["meat_code"] = df_upload["meat_code"].fillna("").astype(str).str.strip()
                df_upload["meat_name"] = df_upload["meat_name"].fillna("").astype(str).str.strip()
                df_upload["origin_grade"] = df_upload["origin_grade"].fillna("").astype(str).str.strip()
                df_upload["tracking_number"] = df_upload["tracking_number"].fillna("").astype(str).str.strip()

                # 유효한 행만
                valid = df_upload[
                    (df_upload["move_date"].notna()) &
                    (df_upload["meat_name"] != "") &
                    (df_upload["kg"] > 0)
                ].copy()

                if valid.empty:
                    st.warning("유효한 데이터가 없습니다. 이동일자, 원육명, Kg를 확인해주세요.")
                else:
                    st.success(f"총 {len(valid)}건의 유효한 데이터가 확인되었습니다.")

                    # 미리보기
                    preview = valid[["move_date", "meat_code", "meat_name", "origin_grade", "kg", "tracking_number"]].copy()
                    preview = preview.rename(columns={
                        "move_date": "이동일자",
                        "meat_code": "원육코드",
                        "meat_name": "원육명",
                        "origin_grade": "원산지(등급)",
                        "kg": "Kg",
                        "tracking_number": "이력번호",
                    })
                    st.dataframe(preview, use_container_width=True, hide_index=True)

                    if st.button("💾 업로드 확정", type="primary", use_container_width=True):
                        rows = []
                        for _, r in valid.iterrows():
                            rows.append({
                                "move_date": r["move_date"],
                                "meat_code": r["meat_code"],
                                "meat_name": r["meat_name"],
                                "origin_grade": r["origin_grade"],
                                "kg": float(r["kg"]),
                                "tracking_number": r["tracking_number"],
                                "product_name": "",
                                "production_kg": 0.0,
                                "memo": "",
                                "completed": False,
                            })
                        try:
                            insert_raw_meat_inputs(rows)
                            st.session_state["_upload_success"] = f"✅ {len(rows)}건 업로드 완료!"
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ 업로드 실패: {str(e)}")

        except Exception as e:
            st.error(f"❌ 파일 읽기 실패: {str(e)}")

    # 업로드 성공 메시지
    if st.session_state.get("_upload_success"):
        st.success(st.session_state["_upload_success"])
        del st.session_state["_upload_success"]


# ========================
# 투입 현황 / 제품 할당
# ========================

elif menu == "📋 투입 현황 / 제품 할당":
    st.subheader("📋 투입 현황 / 제품 할당")

    # 성공 메시지 표시
    for msg_key in ["_assign_success", "_edit_success", "_delete_success"]:
        if st.session_state.get(msg_key):
            st.success(st.session_state[msg_key])
            del st.session_state[msg_key]

    df = load_raw_meat_inputs()

    if df.empty:
        st.info("투입된 원육이 없습니다. '원육 업로드'에서 먼저 업로드해주세요.")
    else:
        products_df = load_products()
        product_options = []
        if not products_df.empty:
            product_options = products_df.apply(
                lambda r: f"{r['product_code']} | {r['product_name']}", axis=1
            ).tolist()

        # 미할당건 (제품 미지정)
        unassigned = df[
            (df["product_name"].fillna("").astype(str).str.strip() == "") |
            (df["completed"] == False)
        ].copy()

        assigned = df[
            (df["product_name"].fillna("").astype(str).str.strip() != "") &
            (df["completed"] == True)
        ].copy()

        # ── 미할당건 (이동일자별)
        if not unassigned.empty:
            st.markdown(f"#### ⚠️ 미할당 건 ({len(unassigned)}건)")

            # 이동일자별 그룹핑
            dates = sorted(unassigned["move_date"].dropna().unique().tolist(), reverse=True)

            for move_date_val in dates:
                date_rows = unassigned[unassigned["move_date"] == move_date_val]
                st.markdown(f"**📅 {move_date_val}** ({len(date_rows)}건)")

                for _, row in date_rows.iterrows():
                    rid = row["id"]
                    meat_name = str(row.get("meat_name", "")).strip()
                    origin = str(row.get("origin_grade", "")).strip()
                    tracking = str(row.get("tracking_number", "")).strip()
                    kg = float(row.get("kg", 0) or 0)
                    current_product = str(row.get("product_name", "")).strip()

                    # 제품이 할당되어 있으면 제품명을, 아니면 원육명 표시
                    if current_product:
                        label = f"🔹 {current_product}"
                    else:
                        label = f"🔸 {meat_name}"
                    if origin:
                        label += f" ({origin})"
                    label += f" | {kg}kg"

                    with st.expander(label, expanded=False):
                        # 읽기 전용 원육 정보
                        info_col1, info_col2 = st.columns(2)
                        with info_col1:
                            st.text_input("원육명", value=meat_name, disabled=True, key=f"ro_meat_{rid}")
                            st.text_input("이력번호", value=tracking, disabled=True, key=f"ro_track_{rid}")
                        with info_col2:
                            st.text_input("원산지(등급)", value=origin, disabled=True, key=f"ro_origin_{rid}")
                            st.text_input("투입량(kg)", value=f"{kg}", disabled=True, key=f"ro_kg_{rid}")

                        st.divider()

                        # st.form으로 감싸서 입력 중 rerun 방지
                        with st.form(key=f"assign_form_{rid}"):
                            # 제품 할당 입력
                            if product_options:
                                current_idx = None
                                if current_product:
                                    for i, opt in enumerate(product_options):
                                        if current_product in opt:
                                            current_idx = i
                                            break
                                sel_product = st.selectbox(
                                    "생산할 제품",
                                    options=product_options,
                                    index=current_idx,
                                    placeholder="제품을 선택하세요...",
                                    key=f"assign_product_{rid}"
                                )
                            else:
                                sel_product = st.text_input("생산할 제품", value=current_product, key=f"assign_product_{rid}")

                            col_a, col_b = st.columns(2)
                            with col_a:
                                prod_kg = st.number_input(
                                    "생산량(kg)", min_value=0.0,
                                    value=float(row.get("production_kg", 0) or 0),
                                    step=0.1, format="%.1f", key=f"assign_kg_{rid}"
                                )
                            with col_b:
                                memo = st.text_input(
                                    "메모",
                                    value=str(row.get("memo", "")).strip(),
                                    key=f"assign_memo_{rid}"
                                )

                            submitted = st.form_submit_button("💾 저장", type="primary", use_container_width=True)

                        # 로스율 미리보기 (form 밖 — session_state에서 현재 값 읽기)
                        form_kg = st.session_state.get(f"assign_kg_{rid}", 0.0)
                        if kg > 0 and form_kg > 0:
                            loss_kg = kg - form_kg
                            loss_rate = round(loss_kg / kg * 100, 2)
                            if loss_rate >= 0:
                                st.info(f"📊 로스율: **{loss_rate}%** | 로스: **{round(loss_kg, 2)}kg**")
                            else:
                                st.warning(f"⚠️ 생산량이 투입량보다 큽니다 (로스율: {loss_rate}%)")

                        # 저장 처리
                        if submitted:
                            try:
                                # 제품명 추출
                                if sel_product and isinstance(sel_product, str) and " | " in sel_product:
                                    p_name = sel_product.split(" | ", 1)[1].strip()
                                elif sel_product:
                                    p_name = str(sel_product).strip()
                                else:
                                    p_name = ""

                                update_data = {
                                    "product_name": p_name,
                                    "production_kg": float(prod_kg),
                                    "memo": memo.strip() if memo else "",
                                    "completed": True if (p_name and prod_kg > 0) else False,
                                }
                                update_raw_meat_input(rid, update_data)
                                st.session_state["_assign_success"] = f"✅ '{p_name or meat_name}' 저장 완료!"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 저장 실패: {str(e)}")

                        # 삭제 버튼 (form 밖)
                        if st.button("🗑️ 삭제", key=f"assign_del_{rid}", use_container_width=False):
                            try:
                                delete_raw_meat_input(rid)
                                st.session_state["_delete_success"] = "✅ 삭제 완료"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 삭제 실패: {str(e)}")

            st.divider()

        # ── 할당 완료건
        if not assigned.empty:
            st.markdown(f"#### ✅ 할당 완료 건 ({len(assigned)}건)")

            # ── 필터 영역
            filter_col1, filter_col2, filter_col3 = st.columns(3)

            all_dates = sorted(assigned["move_date"].dropna().unique().tolist(), reverse=True)
            all_products = sorted(assigned["product_name"].fillna("").astype(str).str.strip().unique().tolist())
            all_products = [p for p in all_products if p]
            all_meats = sorted(assigned["meat_name"].fillna("").astype(str).str.strip().unique().tolist())
            all_meats = [m for m in all_meats if m]

            with filter_col1:
                sel_dates = st.multiselect(
                    "📅 날짜 필터",
                    options=all_dates,
                    default=[],
                    placeholder="전체 날짜",
                    key="filter_assigned_dates"
                )
            with filter_col2:
                sel_products_filter = st.multiselect(
                    "📦 제품 필터",
                    options=all_products,
                    default=[],
                    placeholder="전체 제품",
                    key="filter_assigned_products"
                )
            with filter_col3:
                sel_meats_filter = st.multiselect(
                    "🥩 원육 필터",
                    options=all_meats,
                    default=[],
                    placeholder="전체 원육",
                    key="filter_assigned_meats"
                )

            # 필터 적용
            filtered_assigned = assigned.copy()
            if sel_dates:
                filtered_assigned = filtered_assigned[filtered_assigned["move_date"].isin(sel_dates)]
            if sel_products_filter:
                filtered_assigned = filtered_assigned[
                    filtered_assigned["product_name"].fillna("").astype(str).str.strip().isin(sel_products_filter)
                ]
            if sel_meats_filter:
                filtered_assigned = filtered_assigned[
                    filtered_assigned["meat_name"].fillna("").astype(str).str.strip().isin(sel_meats_filter)
                ]

            active_filters = []
            if sel_dates:
                active_filters.append(f"날짜 {len(sel_dates)}개")
            if sel_products_filter:
                active_filters.append(f"제품 {len(sel_products_filter)}개")
            if sel_meats_filter:
                active_filters.append(f"원육 {len(sel_meats_filter)}개")

            if active_filters:
                st.caption(f"🔍 필터 적용: {', '.join(active_filters)} → **{len(filtered_assigned)}건** 표시")
            else:
                st.caption(f"전체 **{len(filtered_assigned)}건** 표시")

            if filtered_assigned.empty:
                st.info("필터 조건에 해당하는 데이터가 없습니다.")
            else:
                # 날짜별 그룹핑
                a_dates = sorted(filtered_assigned["move_date"].dropna().unique().tolist(), reverse=True)

                for move_date_val in a_dates:
                    date_rows = filtered_assigned[filtered_assigned["move_date"] == move_date_val]
                    st.markdown(f"**📅 {move_date_val}** ({len(date_rows)}건)")

                    # 요약 테이블
                    summary_data = []
                    for _, row in date_rows.iterrows():
                        kg = float(row.get("kg", 0) or 0)
                        prod_kg = float(row.get("production_kg", 0) or 0)
                        loss_kg = round(kg - prod_kg, 2) if kg > 0 and prod_kg > 0 else 0
                        loss_rate = round(loss_kg / kg * 100, 2) if kg > 0 and prod_kg > 0 else None
                        summary_data.append({
                            "제품명": row.get("product_name", ""),
                            "원육명": row.get("meat_name", ""),
                            "원산지(등급)": row.get("origin_grade", ""),
                            "이력번호": row.get("tracking_number", ""),
                            "투입(kg)": kg,
                            "생산(kg)": prod_kg,
                            "로스(kg)": loss_kg,
                            "로스율(%)": loss_rate,
                            "메모": row.get("memo", ""),
                        })

                    summary_df = pd.DataFrame(summary_data)
                    st.dataframe(
                        summary_df.style.format({
                            "투입(kg)": "{:,.1f}",
                            "생산(kg)": "{:,.1f}",
                            "로스(kg)": "{:,.1f}",
                            "로스율(%)": "{:.1f}",
                        }, na_rep="-"),
                        use_container_width=True, hide_index=True
                    )

                    # 수정용 expander
                    for _, row in date_rows.iterrows():
                        rid = row["id"]
                        p_name = str(row.get("product_name", "")).strip()
                        meat_name = str(row.get("meat_name", "")).strip()
                        origin = str(row.get("origin_grade", "")).strip()
                        tracking = str(row.get("tracking_number", "")).strip()
                        kg = float(row.get("kg", 0) or 0)

                        with st.expander(f"✏️ {p_name} (원육: {meat_name})", expanded=False):
                            # 읽기 전용 원육 정보
                            e_col1, e_col2 = st.columns(2)
                            with e_col1:
                                st.text_input("원육명", value=meat_name, disabled=True, key=f"ed_meat_{rid}")
                                st.text_input("이력번호", value=tracking, disabled=True, key=f"ed_track_{rid}")
                            with e_col2:
                                st.text_input("원산지(등급)", value=origin, disabled=True, key=f"ed_origin_{rid}")
                                st.text_input("투입량(kg)", value=f"{kg}", disabled=True, key=f"ed_kg_{rid}")

                            st.divider()

                            # st.form으로 감싸서 입력 중 rerun 방지
                            with st.form(key=f"edit_form_{rid}"):
                                # 수정 가능 필드
                                if product_options:
                                    current_idx = None
                                    for i, opt in enumerate(product_options):
                                        if p_name in opt:
                                            current_idx = i
                                            break
                                    edit_product = st.selectbox(
                                        "생산할 제품",
                                        options=product_options,
                                        index=current_idx,
                                        key=f"edit_product_{rid}"
                                    )
                                else:
                                    edit_product = st.text_input("생산할 제품", value=p_name, key=f"edit_product_{rid}")

                                e_col_a, e_col_b = st.columns(2)
                                with e_col_a:
                                    edit_prod_kg = st.number_input(
                                        "생산량(kg)", min_value=0.0,
                                        value=float(row.get("production_kg", 0) or 0),
                                        step=0.1, format="%.1f", key=f"edit_kg_{rid}"
                                    )
                                with e_col_b:
                                    edit_memo = st.text_input(
                                        "메모",
                                        value=str(row.get("memo", "")).strip(),
                                        key=f"edit_memo_{rid}"
                                    )

                                edit_submitted = st.form_submit_button("💾 수정 저장", type="primary", use_container_width=True)

                            # 로스율 미리보기 (form 밖 — session_state에서 현재 값 읽기)
                            edit_form_kg = st.session_state.get(f"edit_kg_{rid}", 0.0)
                            if kg > 0 and edit_form_kg > 0:
                                loss_kg = kg - edit_form_kg
                                loss_rate = round(loss_kg / kg * 100, 2)
                                if loss_rate >= 0:
                                    st.info(f"📊 로스율: **{loss_rate}%** | 로스: **{round(loss_kg, 2)}kg**")
                                else:
                                    st.warning(f"⚠️ 생산량이 투입량보다 큽니다 (로스율: {loss_rate}%)")

                            # 수정 저장 처리
                            if edit_submitted:
                                try:
                                    if edit_product and isinstance(edit_product, str) and " | " in edit_product:
                                        new_p_name = edit_product.split(" | ", 1)[1].strip()
                                    elif edit_product:
                                        new_p_name = str(edit_product).strip()
                                    else:
                                        new_p_name = ""

                                    update_data = {
                                        "product_name": new_p_name,
                                        "production_kg": float(edit_prod_kg),
                                        "memo": edit_memo.strip() if edit_memo else "",
                                        "completed": True if (new_p_name and edit_prod_kg > 0) else False,
                                    }
                                    update_raw_meat_input(rid, update_data)
                                    st.session_state["_edit_success"] = f"✅ '{new_p_name}' 수정 완료!"
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 수정 실패: {str(e)}")

                            # 삭제 버튼 (form 밖)
                            if st.button("🗑️ 삭제", key=f"edit_del_{rid}", use_container_width=False):
                                try:
                                    delete_raw_meat_input(rid)
                                    st.session_state["_delete_success"] = "✅ 삭제 완료"
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 삭제 실패: {str(e)}")

                    st.divider()

        # ── 전체 요약 메트릭
        if not df.empty:
            st.markdown("#### 📊 전체 요약")
            total = len(df)
            completed_count = len(assigned) if not assigned.empty else 0
            pending_count = len(unassigned) if not unassigned.empty else 0

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("전체 건수", f"{total}건")
            with col2:
                st.metric("할당 완료", f"{completed_count}건")
            with col3:
                st.metric("미할당", f"{pending_count}건")
            with col4:
                total_kg = df["kg"].fillna(0).astype(float).sum()
                st.metric("총 투입량", f"{total_kg:,.1f}kg")
