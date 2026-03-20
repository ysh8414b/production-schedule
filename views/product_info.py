import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from io import BytesIO
from datetime import date, timedelta
from views.sales import load_product_rawmeats, sync_product_rawmeats
from views.loss_data_db import (
    load_production_status_uploads,
    load_production_status_groups,
    load_production_status_items,
)
from utils.auth import get_supabase_client, is_authenticated, can_edit, can_access

import matplotlib.font_manager as fm
import os as _os

def _setup_korean_font():
    """한글 폰트 설정 (로컬: 맑은고딕, Cloud: 나눔고딕)"""
    # Streamlit Cloud에 설치된 나눔고딕 경로
    _nanum_paths = [
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
    ]
    # 로컬 우선 → Cloud 폴백
    _preferred = ["Malgun Gothic", "맑은 고딕"]
    for fn in _preferred:
        hits = [f for f in fm.fontManager.ttflist if fn.lower() in f.name.lower()]
        if hits:
            matplotlib.rcParams["font.family"] = hits[0].name
            matplotlib.rcParams["axes.unicode_minus"] = False
            return
    # Cloud: 나눔고딕 ttf 직접 등록
    for p in _nanum_paths:
        if _os.path.exists(p):
            fm.fontManager.addfont(p)
            fprop = fm.FontProperties(fname=p)
            matplotlib.rcParams["font.family"] = fprop.get_name()
            matplotlib.rcParams["axes.unicode_minus"] = False
            return
    # 최후 폴백
    matplotlib.rcParams["axes.unicode_minus"] = False

_setup_korean_font()

# ========================
# Supabase 연결
# ========================

supabase = get_supabase_client()

# ========================
# uploaded_products DB 함수
# ========================

@st.cache_data(ttl=120)
def load_uploaded_products():
    """uploaded_products 테이블에서 제품 로드 (캐시 2분)"""
    try:
        result = supabase.table("uploaded_products").select("*").order("id").execute()
        if result.data:
            df = pd.DataFrame(result.data)
            if "packs_per_box" not in df.columns:
                df["packs_per_box"] = 0
            if "kg_per_box" not in df.columns:
                df["kg_per_box"] = 0
            return df
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "product_code", "product_name", "origin", "packs_per_box", "kg_per_box",
        "production_time_per_unit", "production_point", "minimum_production_quantity",
        "current_stock"
    ])


def upsert_uploaded_products_bulk(rows):
    """제품 일괄 등록/수정 (product_code 기준 upsert)"""
    client = get_supabase_client()
    chunk_size = 500
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i + chunk_size]
        client.table("uploaded_products").upsert(
            chunk, on_conflict="product_code"
        ).execute()
    load_uploaded_products.clear()
    _clear_schedule_caches()


def delete_uploaded_product(product_id):
    """제품 삭제"""
    client = get_supabase_client()
    client.table("uploaded_products").delete().eq("id", product_id).execute()
    load_uploaded_products.clear()
    _clear_schedule_caches()


def update_uploaded_product_stocks_bulk(updates):
    """여러 제품 재고 일괄 업데이트. updates: list of dict with product_code, current_stock"""
    if not updates:
        return
    client = get_supabase_client()
    for item in updates:
        client.table("uploaded_products").update(
            {"current_stock": int(item["current_stock"])}
        ).eq("product_code", item["product_code"]).execute()
    load_uploaded_products.clear()
    _clear_schedule_caches()


def _clear_schedule_caches():
    """스케줄 페이지 캐시 클리어"""
    try:
        from views.schedule import load_inventory_from_db, load_all_product_names
        load_inventory_from_db.clear()
        load_all_product_names.clear()
    except Exception:
        pass


# ========================
# 페이지 렌더링
# ========================

st.title("📦 제품")

tab1, tab2, tab3, tab4 = st.tabs(["📤 제품 업로드", "📦 제품-원육 매핑", "📊 원육 사용량", "📦 재고 현황"])

# ========================
# Tab 1: 제품 업로드
# ========================

with tab1:
    _tab1_menu_options = ["📋 제품 목록", "📥 엑셀 다운로드"]
    if can_edit("product_info_upload"):
        _tab1_menu_options = ["📋 제품 목록", "📤 엑셀 업로드", "📥 엑셀 다운로드", "📦 재고 관리"]
    menu = st.radio("선택", _tab1_menu_options, horizontal=True, key="uploaded_product_menu")

    st.divider()

    # 성공 메시지
    if st.session_state.get("_product_upload_success"):
        st.success(st.session_state["_product_upload_success"])
        del st.session_state["_product_upload_success"]

    # ── 제품 목록 ──
    if menu == "📋 제품 목록":
        st.subheader("등록된 제품 목록")
        st.caption("제품 업로드 탭에서 등록된 제품입니다. 스케줄 생성 시 이 데이터를 사용합니다.")

        df = load_uploaded_products()

        if df.empty:
            st.info("등록된 제품이 없습니다. '엑셀 업로드'에서 먼저 제품을 등록해주세요.")
        else:
            # 검색
            search = st.text_input("🔍 제품 검색", placeholder="제품코드 또는 제품명 입력...", key="up_prod_search")
            filtered = df.copy()
            if search:
                mask = (
                    filtered["product_name"].astype(str).str.contains(search, case=False, na=False) |
                    filtered["product_code"].astype(str).str.contains(search, case=False, na=False)
                )
                filtered = filtered[mask]

            # 메트릭
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("제품 수", f"{len(filtered)}개")
            with col2:
                has_min_qty = (filtered["minimum_production_quantity"].fillna(0).astype(int) > 0).sum()
                st.metric("최소생산수량 설정", f"{has_min_qty}개")
            with col3:
                origins = filtered["origin"].fillna("").astype(str).str.strip()
                st.metric("원산지 종류", f"{origins[origins != ''].nunique()}개")
            with col4:
                total_stock = filtered["current_stock"].fillna(0).astype(int).sum()
                st.metric("총 재고", f"{total_stock:,}개")

            st.divider()

            if filtered.empty:
                st.info("검색 결과가 없습니다.")
            else:
                # 인라인 편집 가능 테이블
                edit_df = filtered[["product_code", "product_name", "origin", "packs_per_box", "kg_per_box",
                                    "production_time_per_unit", "production_point",
                                    "minimum_production_quantity", "current_stock"]].copy()

                edit_df["packs_per_box"] = pd.to_numeric(edit_df["packs_per_box"], errors="coerce").fillna(0)
                edit_df["kg_per_box"] = pd.to_numeric(edit_df["kg_per_box"], errors="coerce").fillna(0)
                edit_df["production_time_per_unit"] = edit_df["production_time_per_unit"].fillna(0).astype(int)
                edit_df["production_point"] = edit_df["production_point"].fillna("주야").astype(str)
                edit_df["minimum_production_quantity"] = edit_df["minimum_production_quantity"].fillna(0).astype(int)
                edit_df["current_stock"] = edit_df["current_stock"].fillna(0).astype(int)

                edit_df = edit_df.rename(columns={
                    "product_code": "상품코드",
                    "product_name": "상품명",
                    "origin": "원산지",
                    "packs_per_box": "박스당팩수",
                    "kg_per_box": "박스당kg",
                    "production_time_per_unit": "생산시간(초)",
                    "production_point": "생산시점",
                    "minimum_production_quantity": "최소생산수량",
                    "current_stock": "현재고",
                })

                edited = st.data_editor(
                    edit_df,
                    use_container_width=True,
                    hide_index=True,
                    key="uploaded_prod_editor",
                    disabled=["상품코드", "상품명"],
                    column_config={
                        "상품코드": st.column_config.TextColumn("상품코드", width="medium"),
                        "상품명": st.column_config.TextColumn("상품명", width="large"),
                        "원산지": st.column_config.TextColumn("원산지", width="small"),
                        "박스당팩수": st.column_config.NumberColumn("박스당팩수", width="small", min_value=0, format="%.0f"),
                        "박스당kg": st.column_config.NumberColumn("박스당kg", width="small", min_value=0, format="%.2f"),
                        "생산시간(초)": st.column_config.NumberColumn("생산시간(초)", width="small", min_value=0, step=1),
                        "생산시점": st.column_config.SelectboxColumn("생산시점", width="small", options=["주야", "주", "야"]),
                        "최소생산수량": st.column_config.NumberColumn("최소생산수량", width="small", min_value=0, step=1),
                        "현재고": st.column_config.NumberColumn("현재고", width="small", min_value=0, step=1),
                    }
                )

                # 변경 감지
                original = edit_df.reset_index(drop=True)
                changed = edited.reset_index(drop=True)

                diff_mask = (
                    (original["원산지"] != changed["원산지"]) |
                    (original["박스당팩수"] != changed["박스당팩수"]) |
                    (original["박스당kg"] != changed["박스당kg"]) |
                    (original["생산시간(초)"] != changed["생산시간(초)"]) |
                    (original["생산시점"].astype(str) != changed["생산시점"].astype(str)) |
                    (original["최소생산수량"] != changed["최소생산수량"]) |
                    (original["현재고"] != changed["현재고"])
                )
                changed_rows = changed[diff_mask]

                if len(changed_rows) > 0:
                    st.info(f"✏️ **{len(changed_rows)}개** 제품이 수정되었습니다.")
                    if can_edit("product_info_upload"):
                        if st.button("💾 변경사항 저장", type="primary", key="save_uploaded_prod"):
                            client = get_supabase_client()
                            for _, row in changed_rows.iterrows():
                                client.table("uploaded_products").update({
                                    "origin": str(row["원산지"]).strip(),
                                    "packs_per_box": float(row["박스당팩수"]),
                                    "kg_per_box": float(row["박스당kg"]),
                                    "production_time_per_unit": int(row["생산시간(초)"]),
                                    "production_point": str(row["생산시점"]).strip(),
                                    "minimum_production_quantity": int(row["최소생산수량"]),
                                    "current_stock": int(row["현재고"]),
                                }).eq("product_code", row["상품코드"]).execute()
                            load_uploaded_products.clear()
                            _clear_schedule_caches()
                            st.success(f"✅ {len(changed_rows)}개 제품 수정 완료!")
                            st.rerun()

            # 삭제
            if can_edit("product_info_upload"):
                st.divider()
                st.subheader("🗑️ 제품 삭제")
                if not df.empty:
                    delete_options = (df["product_code"].astype(str) + " - " + df["product_name"].astype(str)).tolist()
                    delete_targets = st.multiselect(
                        "삭제할 제품 선택", options=delete_options,
                        placeholder="제품을 선택하세요...", key="up_prod_delete_targets"
                    )
                    if delete_targets:
                        st.warning(f"⚠️ 선택된 {len(delete_targets)}개 제품이 삭제됩니다.")
                        if st.button(f"🗑️ {len(delete_targets)}개 삭제", type="primary", key="up_prod_delete_btn"):
                            delete_codes = [t.split(" - ")[0].strip() for t in delete_targets]
                            delete_ids = df[df["product_code"].isin(delete_codes)]["id"].tolist()
                            if delete_ids:
                                try:
                                    client = get_supabase_client()
                                    for i in range(0, len(delete_ids), 100):
                                        chunk = delete_ids[i:i + 100]
                                        client.table("uploaded_products").delete().in_("id", chunk).execute()
                                    load_uploaded_products.clear()
                                    _clear_schedule_caches()
                                    st.success(f"✅ {len(delete_ids)}개 제품 삭제 완료!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 삭제 실패: {str(e)}")

    # ── 엑셀 업로드 ──
    elif menu == "📤 엑셀 업로드":
        st.subheader("제품 엑셀 업로드")
        st.caption("엑셀/CSV 파일로 제품을 업로드합니다. 이미 존재하는 상품코드는 자동으로 수정됩니다.")
        st.markdown("""
        **업로드 양식 (컬럼명)**
        | 상품코드 | 상품명 | 원산지 | 박스당팩수 | 박스당kg | 생산시간(초) | 생산시점 | 최소생산수량 |
        |---------|-------|-------|---------|---------|------------|---------|------------|
        | E0000001 | 소삼겹양지 1kg*10 | 미국 | 10 | 10.0 | 120 | 주야 | 5 |
        """)

        uploaded_file = st.file_uploader(
            "엑셀 또는 CSV 파일 업로드",
            type=["xlsx", "xls", "csv"],
            key="product_upload_file"
        )

        if uploaded_file:
            try:
                if uploaded_file.name.endswith(".csv"):
                    df_upload = pd.read_csv(uploaded_file)
                else:
                    df_upload = pd.read_excel(uploaded_file)

                # 컬럼 매핑
                col_map = {}
                for col in df_upload.columns:
                    col_clean = str(col).strip().replace(" ", "")
                    if "상품코드" in col_clean or "제품코드" in col_clean or "코드" == col_clean:
                        col_map[col] = "product_code"
                    elif "상품명" in col_clean or "제품명" in col_clean:
                        if "코드" not in col_clean:
                            col_map[col] = "product_name"
                    elif "원산지" in col_clean:
                        col_map[col] = "origin"
                    elif "박스당팩" in col_clean or "팩수" in col_clean:
                        col_map[col] = "packs_per_box"
                    elif "박스당kg" in col_clean or "박스당kg" in col_clean.lower() or "kg/box" in col_clean.lower():
                        col_map[col] = "kg_per_box"
                    elif "생산시간" in col_clean:
                        col_map[col] = "production_time_per_unit"
                    elif "생산시점" in col_clean:
                        col_map[col] = "production_point"
                    elif "최소생산" in col_clean or "최소수량" in col_clean:
                        col_map[col] = "minimum_production_quantity"

                if col_map:
                    df_upload = df_upload.rename(columns=col_map)

                # 필수 컬럼 확인
                required = ["product_code", "product_name"]
                missing = [c for c in required if c not in df_upload.columns]
                if missing:
                    st.error(f"필수 컬럼이 누락되었습니다: {', '.join(missing)}")
                    st.info("필수: 상품코드, 상품명 / 선택: 원산지, 박스당팩수, 박스당kg, 생산시간(초), 생산시점, 최소생산수량")
                else:
                    # 선택 컬럼 기본값
                    if "origin" not in df_upload.columns:
                        df_upload["origin"] = ""
                    if "packs_per_box" not in df_upload.columns:
                        df_upload["packs_per_box"] = 0
                    if "kg_per_box" not in df_upload.columns:
                        df_upload["kg_per_box"] = 0
                    if "production_time_per_unit" not in df_upload.columns:
                        df_upload["production_time_per_unit"] = 0
                    if "production_point" not in df_upload.columns:
                        df_upload["production_point"] = "주야"
                    if "minimum_production_quantity" not in df_upload.columns:
                        df_upload["minimum_production_quantity"] = 0

                    # 데이터 정리
                    df_upload["product_code"] = df_upload["product_code"].fillna("").astype(str).str.strip()
                    df_upload["product_name"] = df_upload["product_name"].fillna("").astype(str).str.strip()
                    df_upload["origin"] = df_upload["origin"].fillna("").astype(str).str.strip()
                    df_upload["packs_per_box"] = pd.to_numeric(df_upload["packs_per_box"], errors="coerce").fillna(0)
                    df_upload["kg_per_box"] = pd.to_numeric(df_upload["kg_per_box"], errors="coerce").fillna(0)
                    df_upload["production_time_per_unit"] = pd.to_numeric(df_upload["production_time_per_unit"], errors="coerce").fillna(0).astype(int)
                    df_upload["production_point"] = df_upload["production_point"].fillna("주야").astype(str).str.strip()
                    df_upload["minimum_production_quantity"] = pd.to_numeric(df_upload["minimum_production_quantity"], errors="coerce").fillna(0).astype(int)

                    # 유효한 행만
                    valid = df_upload[
                        (df_upload["product_code"] != "") &
                        (df_upload["product_name"] != "")
                    ].copy()

                    if valid.empty:
                        st.warning("유효한 데이터가 없습니다. 상품코드, 상품명을 확인해주세요.")
                    else:
                        st.success(f"총 {len(valid)}건의 유효한 데이터가 확인되었습니다.")

                        # 미리보기
                        preview = valid[["product_code", "product_name", "origin", "packs_per_box", "kg_per_box",
                                         "production_time_per_unit", "production_point",
                                         "minimum_production_quantity"]].copy()
                        preview = preview.rename(columns={
                            "product_code": "상품코드",
                            "product_name": "상품명",
                            "origin": "원산지",
                            "packs_per_box": "박스당팩수",
                            "kg_per_box": "박스당kg",
                            "production_time_per_unit": "생산시간(초)",
                            "production_point": "생산시점",
                            "minimum_production_quantity": "최소생산수량",
                        })
                        st.dataframe(preview, use_container_width=True, hide_index=True)

                        if st.button("💾 업로드 확정", type="primary", use_container_width=True, key="product_upload_confirm"):
                            rows = []
                            for _, r in valid.iterrows():
                                rows.append({
                                    "product_code": r["product_code"],
                                    "product_name": r["product_name"],
                                    "origin": r["origin"],
                                    "packs_per_box": float(r["packs_per_box"]),
                                    "kg_per_box": float(r["kg_per_box"]),
                                    "production_time_per_unit": int(r["production_time_per_unit"]),
                                    "production_point": r["production_point"],
                                    "minimum_production_quantity": int(r["minimum_production_quantity"]),
                                })
                            try:
                                upsert_uploaded_products_bulk(rows)
                                st.session_state["_product_upload_success"] = f"✅ {len(rows)}건 업로드 완료!"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 업로드 실패: {str(e)}")

            except Exception as e:
                st.error(f"❌ 파일 읽기 실패: {str(e)}")

    # ── 엑셀 다운로드 ──
    elif menu == "📥 엑셀 다운로드":
        st.subheader("제품 목록 다운로드")

        df = load_uploaded_products()

        if df.empty:
            st.info("등록된 제품이 없습니다.")
        else:
            st.caption(f"총 {len(df)}개 제품")

            display_df = df[["product_code", "product_name", "origin", "packs_per_box", "kg_per_box",
                             "production_time_per_unit", "production_point",
                             "minimum_production_quantity", "current_stock"]].copy()
            display_df = display_df.rename(columns={
                "product_code": "상품코드",
                "product_name": "상품명",
                "origin": "원산지",
                "packs_per_box": "박스당팩수",
                "kg_per_box": "박스당kg",
                "production_time_per_unit": "생산시간(초)",
                "production_point": "생산시점",
                "minimum_production_quantity": "최소생산수량",
                "current_stock": "현재고",
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)

            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                display_df.to_excel(writer, index=False, sheet_name="제품목록")

            st.download_button(
                label="💾 Excel 다운로드",
                data=output.getvalue(),
                file_name="제품목록_업로드.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="up_prod_download_btn"
            )

    # ── 재고 관리 ──
    elif menu == "📦 재고 관리":
        st.subheader("📦 재고 관리")
        st.caption("💡 '현재고' 셀을 직접 클릭하여 수정한 뒤 저장 버튼을 누르세요.")

        df = load_uploaded_products()

        if df.empty:
            st.info("등록된 제품이 없습니다. '엑셀 업로드'에서 먼저 제품을 등록해주세요.")
        else:
            df["current_stock"] = df["current_stock"].fillna(0).astype(int)

            # 검색
            search = st.text_input("🔍 검색", placeholder="제품코드 또는 제품명 입력...", key="up_inv_search")
            filtered = df.copy()
            if search:
                mask = (
                    filtered["product_name"].astype(str).str.contains(search, case=False, na=False) |
                    filtered["product_code"].astype(str).str.contains(search, case=False, na=False)
                )
                filtered = filtered[mask]

            st.divider()

            # 메트릭
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("제품 수", f"{len(filtered)}개")
            with col2:
                st.metric("총 재고", f"{filtered['current_stock'].sum():,}개")
            with col3:
                zero_stock = (filtered["current_stock"] == 0).sum()
                st.metric("재고 없음", f"{zero_stock}개")

            st.divider()

            if filtered.empty:
                st.info("검색 결과가 없습니다.")
            else:
                edit_df = filtered[["product_code", "product_name", "current_stock"]].copy()
                edit_df["current_stock"] = edit_df["current_stock"].fillna(0).astype(int)
                edit_df = edit_df.rename(columns={
                    "product_code": "상품코드",
                    "product_name": "상품명",
                    "current_stock": "현재고"
                })

                edited = st.data_editor(
                    edit_df,
                    use_container_width=True,
                    hide_index=True,
                    key="up_inventory_editor",
                    disabled=["상품코드", "상품명"],
                    column_config={
                        "상품코드": st.column_config.TextColumn("상품코드", width="medium"),
                        "상품명": st.column_config.TextColumn("상품명", width="large"),
                        "현재고": st.column_config.NumberColumn("현재고", width="medium", min_value=0, step=1, format="%d"),
                    }
                )

                original = edit_df.reset_index(drop=True)
                changed = edited.reset_index(drop=True)
                diff_mask = original["현재고"] != changed["현재고"]
                changed_rows = changed[diff_mask]

                if len(changed_rows) > 0:
                    st.info(f"✏️ **{len(changed_rows)}개** 제품의 재고가 수정되었습니다.")

                    with st.expander("변경 내역 확인", expanded=True):
                        preview = changed_rows.copy()
                        preview["기존 재고"] = original.loc[diff_mask, "현재고"].values
                        preview = preview[["상품코드", "상품명", "기존 재고", "현재고"]]
                        st.dataframe(preview, use_container_width=True, hide_index=True)

                    if st.button("💾 재고 저장", type="primary", key="up_inv_save_btn"):
                        updates = []
                        for _, row in changed_rows.iterrows():
                            stock = row["현재고"]
                            stock = 0 if pd.isna(stock) else int(stock)
                            updates.append({"product_code": row["상품코드"], "current_stock": stock})
                        update_uploaded_product_stocks_bulk(updates)
                        st.success(f"✅ {len(updates)}개 제품 재고 저장 완료!")
                        st.rerun()


# ========================
# Tab 2: 제품-원육 매핑 (기존 기능 유지)
# ========================

with tab2:
    st.subheader("📦 제품-원육 매핑")
    st.caption("로스 데이터에서 자동 생성된 제품-원육 매핑을 확인합니다.")

    # 동기화 버튼 (자동 실행 → 수동 버튼)
    if can_edit("product_info_mapping"):
        if st.button("🔄 매핑 동기화", key="sync_rawmeats_btn", help="로스 데이터 기준으로 제품-원육 매핑을 동기화합니다"):
            with st.spinner("동기화 중..."):
                sync_product_rawmeats()
            st.success("동기화 완료!")
            st.rerun()

    mapping_df = load_product_rawmeats()

    if mapping_df.empty:
        st.info("등록된 데이터가 없습니다. 로스 데이터에서 투입상품 생산현황 업로드 시 자동으로 생성됩니다.")
    else:
        products = sorted(mapping_df["product_name"].unique().tolist())
        meat_count = mapping_df[["meat_code", "meat_name"]].drop_duplicates().shape[0]

        col1, col2 = st.columns(2)
        with col1:
            st.metric("등록 제품 수", f"{len(products)}개")
        with col2:
            st.metric("사용 원육 종류", f"{meat_count}개")

        st.divider()

        search = st.text_input("🔍 제품 검색", placeholder="제품명 입력...", key="product_info_search")

        filtered_products = products
        if search:
            filtered_products = [p for p in products if search.lower() in p.lower()]

        if not filtered_products:
            st.info("검색 결과가 없습니다.")
        else:
            selected = st.selectbox(
                "제품 선택",
                options=filtered_products,
                format_func=lambda p: f"📦 {p} ({len(mapping_df[mapping_df['product_name'] == p])}개 원육)",
                key="product_info_select"
            )

            if selected:
                product_meats = mapping_df[mapping_df["product_name"] == selected]
                display_df = product_meats[["meat_code", "meat_name", "origin_grade"]].copy()
                display_df = display_df.rename(columns={
                    "meat_code": "원육코드",
                    "meat_name": "원육명",
                    "origin_grade": "원산지(등급)",
                })
                st.dataframe(display_df, use_container_width=True, hide_index=True)


# ========================
# Tab 3: 원육 사용량
# ========================

with tab3:
    st.subheader("📊 원육 평균 사용량")
    st.caption("생산일보 업로드 데이터 기반 원육별 가중평균 사용량 통계")

    # 데이터 로드 및 조인
    uploads_df = load_production_status_uploads()

    if uploads_df.empty:
        st.info("생산일보 업로드 데이터가 없습니다. 로스 데이터 > 생산일보 탭에서 엑셀을 업로드해주세요.")
    else:
        # 모든 그룹과 항목 로드
        all_groups = load_production_status_groups()
        all_items = load_production_status_items()

        if all_items.empty:
            st.info("업로드된 항목 데이터가 없습니다.")
        else:
            # raw_meat 항목만 필터
            meat_items = all_items[all_items["item_type"] == "raw_meat"].copy()

            if meat_items.empty:
                st.info("원육 투입 데이터가 없습니다.")
            else:
                # 조인: items → groups → uploads (날짜 매핑)
                if not all_groups.empty:
                    group_upload_map = all_groups[["id", "upload_id"]].rename(
                        columns={"id": "group_id"}
                    )
                    meat_items = meat_items.merge(group_upload_map, on="group_id", how="left")

                    upload_date_map = uploads_df[["id", "upload_date"]].rename(
                        columns={"id": "upload_id"}
                    )
                    meat_items = meat_items.merge(upload_date_map, on="upload_id", how="left")
                else:
                    meat_items["upload_date"] = None

                meat_items["upload_date"] = pd.to_datetime(meat_items["upload_date"], errors="coerce")
                meat_items["meat_name_full"] = meat_items["meat_name"].fillna("").astype(str).str.strip()
                # "냉동우육 부채 SWIFT 3D" → "부채" (육종 접두어 제거 후 원육명만 추출)
                _MEAT_PREFIXES = ("냉동우육", "동결우육", "냉장우육", "냉동우", "동결우", "냉장우",
                                  "냉동돈육", "동결돈육", "냉장돈육", "냉동돈", "동결돈", "냉장돈",
                                  "냉동계육", "동결계육", "냉장계육")
                def _normalize_meat_name(full_name):
                    words = full_name.split()
                    if len(words) < 2:
                        return full_name
                    # 첫 단어가 육종 접두어면 제거하고 두 번째 단어(원육명)만 사용
                    if words[0] in _MEAT_PREFIXES:
                        return words[1]
                    return " ".join(words[:2])
                meat_items["meat_name"] = meat_items["meat_name_full"].apply(_normalize_meat_name)
                meat_items["meat_origin"] = meat_items["meat_origin"].fillna("").astype(str).str.strip()
                meat_items["meat_kg"] = pd.to_numeric(meat_items["meat_kg"], errors="coerce").fillna(0)
                meat_items["meat_amount"] = pd.to_numeric(meat_items["meat_amount"], errors="coerce").fillna(0)

                # 빈 원육명 제외
                meat_items = meat_items[meat_items["meat_name"] != ""]

                if meat_items.empty:
                    st.info("유효한 원육 데이터가 없습니다.")
                else:
                    # 날짜별 + 원육별 일간 사용량 합산 (월~금만)
                    meat_items = meat_items[meat_items["upload_date"].dt.weekday < 5]
                    daily = meat_items.groupby(
                        ["upload_date", "meat_name", "meat_origin"]
                    ).agg(
                        daily_kg=("meat_kg", "sum"),
                        daily_amount=("meat_amount", "sum"),
                    ).reset_index()

                    # 데이터 기준 주차 계산 (시스템 날짜 대신 실제 데이터의 최근 날짜 기준)
                    latest_date = daily["upload_date"].max()
                    data_monday = latest_date - timedelta(days=latest_date.weekday())

                    def calc_week_avg(df, num_weeks):
                        start = pd.Timestamp(data_monday - timedelta(weeks=num_weeks - 1))
                        period = df[df["upload_date"] >= start]
                        if period.empty:
                            return pd.DataFrame(columns=["meat_name", "meat_origin", "avg_kg", "avg_amount", "has_data"])
                        grp = period.groupby(["meat_name", "meat_origin"]).agg(
                            total_kg=("daily_kg", "sum"),
                            total_amount=("daily_amount", "sum"),
                        ).reset_index()
                        grp["avg_kg"] = grp["total_kg"] / num_weeks
                        grp["avg_amount"] = grp["total_amount"] / num_weeks
                        grp["has_data"] = 1
                        return grp[["meat_name", "meat_origin", "avg_kg", "avg_amount", "has_data"]]

                    avg1w = calc_week_avg(daily, 1)
                    avg2w = calc_week_avg(daily, 2)
                    avg4w = calc_week_avg(daily, 4)

                    # 전체 원육 목록
                    all_meats = daily[["meat_name", "meat_origin"]].drop_duplicates()

                    result = all_meats.copy()
                    for suffix, avg_df in [("1w", avg1w), ("2w", avg2w), ("4w", avg4w)]:
                        result = result.merge(
                            avg_df.rename(columns={
                                "avg_kg": f"avg_kg_{suffix}",
                                "avg_amount": f"avg_amount_{suffix}",
                                "has_data": f"has_{suffix}",
                            }),
                            on=["meat_name", "meat_origin"],
                            how="left",
                        )

                    for col in ["avg_kg_1w", "avg_kg_2w", "avg_kg_4w",
                                "avg_amount_1w", "avg_amount_2w", "avg_amount_4w"]:
                        result[col] = result[col].fillna(0)
                    for col in ["has_1w", "has_2w", "has_4w"]:
                        result[col] = result[col].fillna(0).astype(int)

                    # 가중평균 계산 — 데이터가 있는 기간만 가중치 재분배
                    weights = {"1w": 0.5, "2w": 0.3, "4w": 0.2}

                    def weighted_avg(row, value_prefix):
                        active = {k: w for k, w in weights.items() if row[f"has_{k}"] > 0}
                        if not active:
                            return 0.0
                        total_w = sum(active.values())
                        return sum(
                            row[f"{value_prefix}_{k}"] * (w / total_w)
                            for k, w in active.items()
                        )

                    result["weighted_kg"] = result.apply(lambda r: weighted_avg(r, "avg_kg"), axis=1)
                    result["weighted_amount"] = result.apply(lambda r: weighted_avg(r, "avg_amount"), axis=1)

                    # 내림차순 정렬
                    result = result.sort_values("weighted_kg", ascending=False).reset_index(drop=True)

                    # 가중평균 0인 행 제외
                    result = result[result["weighted_kg"] > 0]

                    if result.empty:
                        st.info("최근 30일 내 원육 사용 데이터가 없습니다.")
                    else:
                        # 메트릭
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("원육 종류", f"{len(result)}종")
                        with col2:
                            st.metric("주평균 총사용량", f"{result['weighted_kg'].sum():,.1f}kg")
                        with col3:
                            st.metric("주평균 총금액", f"{result['weighted_amount'].sum():,.0f}원")

                        st.divider()

                        # 테이블 표시
                        display = result[["meat_name", "meat_origin",
                                          "weighted_kg", "weighted_amount"]].copy()
                        display = display.rename(columns={
                            "meat_name": "원육명",
                            "meat_origin": "원산지",
                            "weighted_kg": "주평균 사용량(kg)",
                            "weighted_amount": "주평균 금액(원)",
                        })

                        st.dataframe(
                            display.style.format({
                                "주평균 사용량(kg)": "{:,.1f}",
                                "주평균 금액(원)": "{:,.0f}",
                            }),
                            use_container_width=True, hide_index=True,
                        )

                        # 차트 생성 및 이미지 다운로드
                        st.divider()
                        st.markdown("#### 📈 원육 사용량 차트")

                        chart_data = result.head(20).copy()
                        chart_data["label"] = chart_data.apply(
                            lambda r: f"{r['meat_name']} ({r['meat_origin']})" if r["meat_origin"] else r["meat_name"],
                            axis=1,
                        )

                        fig, ax = plt.subplots(figsize=(12, max(6, len(chart_data) * 0.45)))
                        bars = ax.barh(
                            range(len(chart_data)),
                            chart_data["weighted_kg"].values,
                            color="#4CAF50",
                            edgecolor="white",
                        )
                        ax.set_yticks(range(len(chart_data)))
                        ax.set_yticklabels(chart_data["label"].values, fontsize=9)
                        ax.invert_yaxis()
                        ax.set_xlabel("가중평균 사용량 (kg/주)", fontsize=10)
                        ax.set_title("원육별 주평균 사용량 (가중평균)", fontsize=13, fontweight="bold")

                        for bar, val, amt in zip(bars, chart_data["weighted_kg"], chart_data["weighted_amount"]):
                            ax.text(
                                bar.get_width() + max(chart_data["weighted_kg"]) * 0.01,
                                bar.get_y() + bar.get_height() / 2,
                                f"{val:,.1f}kg  ({amt:,.0f}원)",
                                va="center", fontsize=8,
                            )

                        ax.set_xlim(0, chart_data["weighted_kg"].max() * 1.35)
                        plt.tight_layout()

                        st.pyplot(fig)

                        # 이미지 다운로드
                        img_buf = BytesIO()
                        fig.savefig(img_buf, format="png", dpi=150, bbox_inches="tight")
                        img_buf.seek(0)
                        plt.close(fig)

                        st.download_button(
                            label="📥 이미지 다운로드 (PNG)",
                            data=img_buf.getvalue(),
                            file_name=f"원육사용량_{date.today()}.png",
                            mime="image/png",
                            key="meat_usage_img_download",
                        )


# ========================
# Tab 4: 재고 현황
# ========================


# ── 재고 DB 함수 ──

@st.cache_data(ttl=120)
def _load_inventory_products_db():
    try:
        result = supabase.table("inventory_products").select("*").order("sort_order").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except Exception:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=120)
def _load_inventory_meats_db():
    try:
        result = supabase.table("inventory_meats").select("*").order("sort_order").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except Exception:
        pass
    return pd.DataFrame()


def _save_inventory_products_db(product_rows):
    """제품 재고만 교체 저장 (기존 제품 재고 삭제 → 새로 삽입)"""
    client = get_supabase_client()
    try:
        client.table("inventory_products").delete().neq("product_code", "").execute()
    except Exception:
        pass
    if product_rows:
        for i in range(0, len(product_rows), 500):
            client.table("inventory_products").insert(product_rows[i:i + 500]).execute()
    _load_inventory_products_db.clear()


def _save_inventory_meats_db(meat_rows):
    """원육 재고만 교체 저장 (기존 원육 재고 삭제 → 새로 삽입)"""
    client = get_supabase_client()
    try:
        client.table("inventory_meats").delete().neq("meat_code", "").execute()
    except Exception:
        pass
    if meat_rows:
        for i in range(0, len(meat_rows), 500):
            client.table("inventory_meats").insert(meat_rows[i:i + 500]).execute()
    _load_inventory_meats_db.clear()


# ── 엑셀 파싱 ──

def _parse_inventory_sheet(xls_file, sheet_name):
    """제품 재고 / 원육 재고 시트 파싱"""
    raw = pd.read_excel(xls_file, sheet_name=sheet_name, header=None)
    base_date_str = None
    # A1, A2 및 마지막 열에서 날짜 탐색
    for ri in [0, 1]:
        for ci in [0, raw.shape[1] - 1]:
            if ri >= raw.shape[0] or ci >= raw.shape[1]:
                continue
            cell_val = raw.iloc[ri, ci]
            if pd.isna(cell_val):
                continue
            # datetime 객체인 경우 직접 변환
            if isinstance(cell_val, (pd.Timestamp,)):
                base_date_str = cell_val.strftime("%Y-%m-%d")
                break
            import datetime as _dt
            if isinstance(cell_val, (_dt.datetime, _dt.date)):
                base_date_str = pd.Timestamp(cell_val).strftime("%Y-%m-%d")
                break
            cell = str(cell_val).strip()
            if "기준일자" in cell or "출력일자" in cell:
                parts = cell.split(":")
                if len(parts) > 1:
                    base_date_str = parts[-1].strip()
                    break
            # 날짜 형식 문자열 시도 (예: 2026-03-15, 2026/03/15)
            if not base_date_str and len(cell) >= 6:
                try:
                    parsed = pd.to_datetime(cell)
                    base_date_str = parsed.strftime("%Y-%m-%d")
                    break
                except Exception:
                    pass
        if base_date_str:
            break
    df = raw.iloc[2:].copy()
    df.columns = [
        "상품코드", "상품명", "원산지", "등급", "입고일자", "번호", "구분",
        "입고처명", "입고Box", "입고Kg", "입고단가", "입고금액",
        "잔량Box", "잔량Kg", "잔량금액", "이력번호", "소비기한",
        "잔여일수", "BL번호", "지점명", "비고", "참고"
    ]
    df = df.dropna(subset=["상품코드"])
    df["상품코드"] = df["상품코드"].astype(str).str.strip()
    df = df[df["상품코드"] != ""]
    df["상품명"] = df["상품명"].astype(str).str.strip()
    df["원산지"] = df["원산지"].fillna("").astype(str).str.strip()
    for col in ["입고Kg", "잔량Kg", "입고단가", "입고금액", "잔량금액"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    for col in ["입고Box", "잔량Box", "잔여일수"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df, base_date_str


# ── 제품 배경색 ──

# 제품코드 → 배경색 매핑 (재고 엑셀 시트 기준)
_PRODUCT_CODE_COLORS = {
    # (세) 소포장 - 연노랑
    "F0000069": "#FFE599", "E0000068": "#FFE599", "E0000066": "#FFE599",
    "E0000067": "#FFE599", "F0000070": "#FFE599",
    # (세) 대용량 - 연주황
    "F0000073": "#FCE5CD", "E0000069": "#FCE5CD", "E0000070": "#FCE5CD",
    "F0000074": "#FCE5CD", "F0000071": "#FCE5CD", "F0000072": "#FCE5CD",
    # (마) - 연파랑
    "F0000077": "#CFE2F3", "E0000062": "#CFE2F3", "E0000064": "#CFE2F3",
    "E0000061": "#CFE2F3", "H0000003": "#CFE2F3", "F0000065": "#CFE2F3",
    "F0000064": "#CFE2F3",
    # (쿠) 목전지 - 연초록
    "F0000047": "#D9EAD3", "F0000048": "#D9EAD3", "F0000050": "#D9EAD3",
    "F0000078": "#D9EAD3",
    # (쿠) 돌돌우삼겹 - 연분홍
    "E0000045": "#EAD1DC", "E0000046": "#EAD1DC", "E0000043": "#EAD1DC",
}

# 접두어 기반 폴백 색상 (코드 매핑에 없는 신규 제품용)
_PREFIX_COLORS = {
    "(세)": "#FFE599",
    "(마)": "#CFE2F3",
    "(쿠)": "#D9EAD3",
    "(하)": "#F8BBD0",
}
_DEFAULT_PRODUCT_COLOR = "#FFFFFF"  # 배경색 없는 제품은 흰색


def _get_product_color(product_name, product_code=None):
    if product_code and str(product_code).strip() in _PRODUCT_CODE_COLORS:
        return _PRODUCT_CODE_COLORS[str(product_code).strip()]
    name = str(product_name).strip()
    for prefix, color in _PREFIX_COLORS.items():
        if name.startswith(prefix):
            return color
    return _DEFAULT_PRODUCT_COLOR


# ── 보고서 이미지 생성 ──

def _build_inventory_image(product_df, meat_df, base_date_str, usage_df=None):
    """재고 시트 형태 이미지 (A4 세로, 제품 좌 + 원육 우 나란히)
    usage_df: 원육 사용량 DataFrame (meat_name, meat_origin, weighted_kg) — 있으면 재고<사용량 행 굵게 표시
    """
    import matplotlib
    import matplotlib.font_manager as fm

    # 한글 폰트 (로컬: 맑은고딕, Cloud: 나눔고딕)
    _font_path = None
    _preferred = ["Malgun Gothic", "맑은 고딕"]
    for fn in _preferred:
        hits = [f for f in fm.fontManager.ttflist if fn.lower() in f.name.lower()]
        if hits:
            _font_path = hits[0].fname
            matplotlib.rcParams["font.family"] = hits[0].name
            break
    if not _font_path:
        _nanum_paths = [
            "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
            "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        ]
        for p in _nanum_paths:
            if _os.path.exists(p):
                fm.fontManager.addfont(p)
                _font_path = p
                fprop_tmp = fm.FontProperties(fname=p)
                matplotlib.rcParams["font.family"] = fprop_tmp.get_name()
                break
    matplotlib.rc("axes", unicode_minus=False)

    # FontProperties 객체 생성 (ax.text에서 직접 사용)
    _fprop = fm.FontProperties(fname=_font_path) if _font_path else fm.FontProperties(family="sans-serif")

    # A4 세로: 210mm x 297mm → 8.27 x 11.69 inches
    W, H = 8.27, 11.69

    n_prod = len(product_df)
    n_meat = len(meat_df)
    title_h = 0.5
    margin_bottom = 0.1
    avail_h = H - title_h - margin_bottom
    max_rows = max(n_prod, n_meat) + 1
    row_h = min(avail_h / max_rows, 0.22)

    fig, ax = plt.subplots(figsize=(W, H))
    ax.axis("off")
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)

    fs_title = 14
    fs_header = min(10, row_h * 40)
    fs_data = min(9, row_h * 36)
    fs_stock = min(11, row_h * 44)

    # 타이틀
    ax.text(W / 2, H - 0.15, "4층 제품 & 원육 재고", fontsize=fs_title, fontweight="bold", ha="center", va="top", fontproperties=_fprop)
    if base_date_str:
        try:
            dt = pd.to_datetime(base_date_str)
            weekdays_kr = ["월", "화", "수", "목", "금", "토", "일"]
            date_label = f"{dt.month:02d}/{dt.day:02d} ({weekdays_kr[dt.weekday()]})"
        except Exception:
            date_label = base_date_str
        ax.text(W - 0.2, H - 0.15, date_label, fontsize=9, ha="right", va="top", color="#333", fontweight="bold", fontproperties=_fprop)

    # ═══ 레이아웃 (좌: 제품, 우: 원육) ═══
    gap_mid = 0.2
    margin_x = 0.15
    total_w = W - margin_x * 2 - gap_mid
    L_w = total_w * 0.40   # 제품
    R_w = total_w * 0.60   # 원육
    L_left = margin_x
    R_left = L_left + L_w + gap_mid

    # ── 제품 컬럼 (코드 | 제품명 | 재고) ──
    p_code_x = L_left + 0.03
    p_name_x = L_left + L_w * 0.25
    p_stock_x = L_left + L_w - 0.05

    y = H - title_h - row_h

    # 제품 헤더
    ax.add_patch(plt.Rectangle((L_left, y), L_w, row_h, facecolor="#D6E4F0", edgecolor="#8DB4E2", linewidth=0.5))
    ax.text(p_code_x, y + row_h / 2, "제품코드", fontsize=fs_header, fontweight="bold", va="center", fontproperties=_fprop)
    ax.text(p_name_x, y + row_h / 2, "제품명", fontsize=fs_header, fontweight="bold", va="center", fontproperties=_fprop)
    ax.text(p_stock_x, y + row_h / 2, "현 재고", fontsize=fs_header, fontweight="bold", va="center", ha="right", fontproperties=_fprop)

    for _, row in product_df.iterrows():
        y -= row_h
        bg = _get_product_color(row["product_name"], row.get("product_code"))
        ax.add_patch(plt.Rectangle((L_left, y), L_w, row_h, facecolor=bg, edgecolor="#C0C0C0", linewidth=0.3))

        stock = int(row["remaining_box"])
        s_color = "#FF0000" if stock == 0 else "#000000"
        s_weight = "bold" if stock == 0 else "normal"

        ax.text(p_code_x, y + row_h / 2, str(row["product_code"]), fontsize=fs_data, va="center", color="#555", fontproperties=_fprop)
        ax.text(p_name_x, y + row_h / 2, str(row["product_name"]), fontsize=fs_data, va="center", color="#000", fontproperties=_fprop)
        ax.text(p_stock_x, y + row_h / 2, f"{stock:,}", fontsize=fs_stock, va="center", ha="right",
                color=s_color, fontweight=s_weight, fontproperties=_fprop)

    # ── 원육 컬럼 (코드 | 원육명 | 원산지 | kg | 박스) ──
    m_code_x = R_left + 0.03
    m_name_x = R_left + R_w * 0.22
    m_origin_x = R_left + R_w * 0.66
    m_kg_x = R_left + R_w * 0.89
    m_box_x = R_left + R_w - 0.03

    y = H - title_h - row_h

    # 원육 헤더
    ax.add_patch(plt.Rectangle((R_left, y), R_w, row_h, facecolor="#D6E4F0", edgecolor="#8DB4E2", linewidth=0.5))
    ax.text(m_code_x, y + row_h / 2, "원육코드", fontsize=fs_header, fontweight="bold", va="center", fontproperties=_fprop)
    ax.text(m_name_x, y + row_h / 2, "원육명", fontsize=fs_header, fontweight="bold", va="center", fontproperties=_fprop)
    ax.text(m_origin_x, y + row_h / 2, "원산지", fontsize=fs_header, fontweight="bold", va="center", fontproperties=_fprop)
    ax.text(m_kg_x, y + row_h / 2, "kg", fontsize=fs_header, fontweight="bold", va="center", ha="right", fontproperties=_fprop)
    ax.text(m_box_x, y + row_h / 2, "박스", fontsize=fs_header, fontweight="bold", va="center", ha="right", fontproperties=_fprop)

    # 사용량 매칭용 딕셔너리 구축
    _usage_map = {}
    if usage_df is not None and not usage_df.empty:
        for _, u_row in usage_df.iterrows():
            key = (str(u_row["meat_name"]).strip(), str(u_row["meat_origin"]).strip())
            _usage_map[key] = float(u_row["weighted_kg"])

    _MEAT_PREFIXES_IMG = ("냉동우육", "동결우육", "냉장우육", "냉동우", "동결우", "냉장우",
                          "냉동돈육", "동결돈육", "냉장돈육", "냉동돈", "동결돈", "냉장돈",
                          "냉동계육", "동결계육", "냉장계육")

    def _normalize_for_match(full_name):
        words = str(full_name).strip().split()
        if len(words) < 2:
            return full_name.strip()
        if words[0] in _MEAT_PREFIXES_IMG:
            return words[1]
        return " ".join(words[:2])

    # 원육명+원산지 기준 합산 재고 계산 (같은 원육이 브랜드별로 여러 행일 수 있음)
    _total_inventory = {}
    if _usage_map:
        for _, m_row in meat_df.iterrows():
            _key = (_normalize_for_match(m_row["meat_name"]), str(m_row["origin"]).strip())
            _total_inventory[_key] = _total_inventory.get(_key, 0) + float(m_row["remaining_kg"])

    for idx, (_, row) in enumerate(meat_df.iterrows()):
        y -= row_h
        bg = "#FFFFFF" if idx % 2 == 0 else "#F5F5F5"
        ax.add_patch(plt.Rectangle((R_left, y), R_w, row_h, facecolor=bg, edgecolor="#C0C0C0", linewidth=0.3))

        kg = row["remaining_kg"]
        bx = int(row["remaining_box"])

        # 합산 재고가 평균 사용량보다 적으면 굵게 표시 (캐나다 삼겹/목살 제외)
        _is_low = False
        if _usage_map:
            _norm_name = _normalize_for_match(row["meat_name"])
            _norm_origin = str(row["origin"]).strip()
            _exclude = _norm_origin == "캐나다" and _norm_name in ("삼겹", "목살")
            if not _exclude:
                _avg_kg = _usage_map.get((_norm_name, _norm_origin), None)
                _total_kg = _total_inventory.get((_norm_name, _norm_origin), 0)
                if _avg_kg is not None and _total_kg < _avg_kg:
                    _is_low = True

        _fw = "bold" if _is_low else "normal"
        _fc = "#CC0000" if _is_low else "#000"
        _fc_sub = "#CC0000" if _is_low else "#555"

        ax.text(m_code_x, y + row_h / 2, str(row["meat_code"]), fontsize=fs_data, va="center", color=_fc_sub, fontweight=_fw, fontproperties=_fprop)
        ax.text(m_name_x, y + row_h / 2, str(row["meat_name"]), fontsize=fs_data, va="center", color=_fc, fontweight=_fw, fontproperties=_fprop)
        ax.text(m_origin_x, y + row_h / 2, str(row["origin"]), fontsize=fs_data, va="center", color=_fc_sub, fontweight=_fw, fontproperties=_fprop)
        ax.text(m_kg_x, y + row_h / 2, f"{kg:,.1f}" if kg else "0", fontsize=fs_data, va="center", ha="right", color=_fc, fontweight=_fw, fontproperties=_fprop)
        ax.text(m_box_x, y + row_h / 2, f"{bx:,}", fontsize=fs_data, va="center", ha="right", color=_fc, fontweight=_fw, fontproperties=_fprop)

    plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
    return fig


# ── Tab4 UI ──

with tab4:
    st.subheader("📦 재고 현황")

    # DB에서 저장된 재고 로드
    saved_products = _load_inventory_products_db()
    saved_meats = _load_inventory_meats_db()

    # 업로드 섹션 (제품 / 원육 따로)
    if can_edit("product_info_stock"):
        with st.expander("📤 재고 엑셀 업로드", expanded=saved_products.empty and saved_meats.empty):
            upload_col_l, upload_col_r = st.columns(2)

            # ── 제품 재고 업로드 ──
            with upload_col_l:
                st.markdown("**📦 제품 재고**")
                up_products = load_uploaded_products()
                if up_products.empty:
                    st.warning("먼저 '제품 업로드' 탭에서 제품 목록을 등록해주세요.")
                else:
                    uploaded_prod_inv = st.file_uploader(
                        "제품 재고 엑셀", type=["xlsx", "xls"], key="inv_prod_upload",
                        help="'재고집계' 시트가 포함된 엑셀 파일",
                    )
                    if uploaded_prod_inv:
                        try:
                            xls_p = pd.ExcelFile(uploaded_prod_inv)
                            if "재고집계" not in xls_p.sheet_names:
                                st.warning("'재고집계' 시트를 찾을 수 없습니다.")
                            else:
                                p_detail, p_date = _parse_inventory_sheet(xls_p, "재고집계")
                                # 엑셀에서 상품코드별 잔량 합산
                                excel_stock = p_detail.groupby("상품코드").agg(
                                    잔량Box=("잔량Box", "sum"),
                                ).reset_index()
                                stock_map = dict(zip(excel_stock["상품코드"].astype(str), excel_stock["잔량Box"]))

                                # uploaded_products 기준으로 제품 목록 구성
                                p_agg = pd.DataFrame({
                                    "상품코드": up_products["product_code"].astype(str),
                                    "상품명": up_products["product_name"].astype(str),
                                    "잔량Box": up_products["product_code"].astype(str).map(stock_map).fillna(0).astype(int),
                                })

                                if p_date:
                                    st.caption(f"기준일자: {p_date}")
                                st.dataframe(
                                    p_agg[["상품코드", "상품명", "잔량Box"]].rename(columns={"잔량Box": "현재고"}),
                                    use_container_width=True, hide_index=True, height=250,
                                )
                                if st.button("💾 제품 재고 저장", type="primary", key="inv_prod_save_btn", use_container_width=True):
                                    rows = []
                                    for idx, (_, r) in enumerate(p_agg.iterrows()):
                                        rows.append({
                                            "product_code": str(r["상품코드"]),
                                            "product_name": str(r["상품명"]),
                                            "remaining_box": int(r["잔량Box"]),
                                            "base_date": p_date or "",
                                            "sort_order": idx,
                                        })
                                    try:
                                        _save_inventory_products_db(rows)
                                        stock_updates = [{"product_code": r["product_code"], "current_stock": r["remaining_box"]} for r in rows]
                                        update_uploaded_product_stocks_bulk(stock_updates)
                                        st.session_state["_inv_save_success"] = "제품 재고"
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ 저장 실패: {str(e)}")
                        except Exception as e:
                            st.error(f"❌ 파일 처리 실패: {str(e)}")

            # ── 원육 재고 업로드 ──
            with upload_col_r:
                st.markdown("**🥩 원육 재고**")
                uploaded_meat_inv = st.file_uploader(
                    "원육 재고 엑셀", type=["xlsx", "xls"], key="inv_meat_upload",
                    help="'재고집계' 시트가 포함된 엑셀 파일",
                )
                if uploaded_meat_inv:
                    try:
                        xls_m = pd.ExcelFile(uploaded_meat_inv)
                        if "재고집계" not in xls_m.sheet_names:
                            st.warning("'재고집계' 시트를 찾을 수 없습니다.")
                        else:
                            m_detail, m_date = _parse_inventory_sheet(xls_m, "재고집계")
                            m_agg = m_detail.groupby(["상품코드", "상품명", "원산지"]).agg(
                                잔량Box=("잔량Box", "sum"),
                                잔량Kg=("잔량Kg", "sum"),
                            ).reset_index().sort_values("잔량Kg", ascending=False)

                            if m_date:
                                st.caption(f"기준일자: {m_date}")
                            st.dataframe(
                                m_agg[["상품코드", "상품명", "원산지", "잔량Kg", "잔량Box"]].rename(
                                    columns={"잔량Kg": "kg", "잔량Box": "박스"}
                                ).style.format({"kg": "{:,.1f}"}),
                                use_container_width=True, hide_index=True, height=250,
                            )
                            if st.button("💾 원육 재고 저장", type="primary", key="inv_meat_save_btn", use_container_width=True):
                                rows = []
                                for idx, (_, r) in enumerate(m_agg.iterrows()):
                                    rows.append({
                                        "meat_code": str(r["상품코드"]),
                                        "meat_name": str(r["상품명"]),
                                        "origin": str(r["원산지"]),
                                        "remaining_kg": float(r["잔량Kg"]),
                                        "remaining_box": int(r["잔량Box"]),
                                        "base_date": m_date or "",
                                        "sort_order": idx,
                                    })
                                try:
                                    _save_inventory_meats_db(rows)
                                    st.session_state["_inv_save_success"] = "원육 재고"
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 저장 실패: {str(e)}")
                    except Exception as e:
                        st.error(f"❌ 파일 처리 실패: {str(e)}")

    # 저장 성공 메시지
    _inv_success = st.session_state.pop("_inv_save_success", None)
    if _inv_success:
        st.success(f"✅ {_inv_success} 저장 완료!")
        saved_products = _load_inventory_products_db()
        saved_meats = _load_inventory_meats_db()

    # ── 저장된 재고 표시 ──
    if saved_products.empty and saved_meats.empty:
        st.info("저장된 재고 데이터가 없습니다. 위 '재고 엑셀 업로드'에서 엑셀을 업로드해주세요.")
    else:
        # 기준일자
        inv_base_date = ""
        if not saved_products.empty and "base_date" in saved_products.columns:
            inv_base_date = saved_products["base_date"].iloc[0] if pd.notna(saved_products["base_date"].iloc[0]) else ""
        elif not saved_meats.empty and "base_date" in saved_meats.columns:
            inv_base_date = saved_meats["base_date"].iloc[0] if pd.notna(saved_meats["base_date"].iloc[0]) else ""

        if inv_base_date:
            st.info(f"📅 기준일자: **{inv_base_date}**")

        # 메트릭
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("제품 종류", f"{len(saved_products)}종" if not saved_products.empty else "-")
        with c2:
            prod_total = int(saved_products["remaining_box"].sum()) if not saved_products.empty else 0
            st.metric("제품 총재고", f"{prod_total:,}Box")
        with c3:
            st.metric("원육 종류", f"{len(saved_meats)}종" if not saved_meats.empty else "-")
        with c4:
            meat_total = saved_meats["remaining_kg"].sum() if not saved_meats.empty else 0
            st.metric("원육 총재고", f"{meat_total:,.1f}kg")

        st.divider()

        inv_sub = st.radio(
            "보기", ["📊 재고 보고서", "📦 제품 상세", "🥩 원육 상세"],
            horizontal=True, key="inv_sub_menu"
        )

        # ── 재고 보고서 ──
        if inv_sub == "📊 재고 보고서":
            # 원육코드에 숫자가 포함되지 않은 행 제외
            filtered_meats = saved_meats[saved_meats["meat_code"].astype(str).str.contains(r'\d', na=False)].copy() if not saved_meats.empty else saved_meats

            # 원육 사용량 데이터 로드 (재고 < 평균사용량 행 굵게 표시용)
            _usage_for_report = None
            try:
                _u_uploads = load_production_status_uploads()
                if not _u_uploads.empty:
                    _u_groups = load_production_status_groups()
                    _u_items = load_production_status_items()
                    if not _u_items.empty:
                        _u_meat = _u_items[_u_items["item_type"] == "raw_meat"].copy()
                        if not _u_meat.empty and not _u_groups.empty:
                            _u_meat = _u_meat.merge(
                                _u_groups[["id", "upload_id"]].rename(columns={"id": "group_id"}),
                                on="group_id", how="left"
                            )
                            _u_meat = _u_meat.merge(
                                _u_uploads[["id", "upload_date"]].rename(columns={"id": "upload_id"}),
                                on="upload_id", how="left"
                            )
                            _u_meat["upload_date"] = pd.to_datetime(_u_meat["upload_date"], errors="coerce")
                            _u_meat["meat_kg"] = pd.to_numeric(_u_meat["meat_kg"], errors="coerce").fillna(0)
                            _u_meat["meat_name"] = _u_meat["meat_name"].fillna("").astype(str).str.strip()
                            _u_meat["meat_origin"] = _u_meat["meat_origin"].fillna("").astype(str).str.strip()
                            # 원육명 정규화
                            _PREFIXES = ("냉동우육", "동결우육", "냉장우육", "냉동우", "동결우", "냉장우",
                                         "냉동돈육", "동결돈육", "냉장돈육", "냉동돈", "동결돈", "냉장돈",
                                         "냉동계육", "동결계육", "냉장계육")
                            def _norm(n):
                                words = n.split()
                                if len(words) < 2:
                                    return n
                                return words[1] if words[0] in _PREFIXES else " ".join(words[:2])
                            _u_meat["meat_name"] = _u_meat["meat_name"].apply(_norm)
                            _u_meat = _u_meat[(_u_meat["meat_name"] != "") & (_u_meat["upload_date"].dt.weekday < 5)]
                            _u_daily = _u_meat.groupby(["upload_date", "meat_name", "meat_origin"]).agg(
                                daily_kg=("meat_kg", "sum")).reset_index()
                            _latest = _u_daily["upload_date"].max()
                            _monday = _latest - timedelta(days=_latest.weekday())
                            _weights = {"1w": 0.5, "2w": 0.3, "4w": 0.2}
                            _all_m = _u_daily[["meat_name", "meat_origin"]].drop_duplicates()
                            _res = _all_m.copy()
                            for _sfx, _nw in [("1w", 1), ("2w", 2), ("4w", 4)]:
                                _start = pd.Timestamp(_monday - timedelta(weeks=_nw - 1))
                                _p = _u_daily[_u_daily["upload_date"] >= _start]
                                if not _p.empty:
                                    _g = _p.groupby(["meat_name", "meat_origin"]).agg(total=("daily_kg", "sum")).reset_index()
                                    _g[f"avg_{_sfx}"] = _g["total"] / _nw
                                    _g[f"has_{_sfx}"] = 1
                                    _res = _res.merge(_g[["meat_name", "meat_origin", f"avg_{_sfx}", f"has_{_sfx}"]],
                                                      on=["meat_name", "meat_origin"], how="left")
                                else:
                                    _res[f"avg_{_sfx}"] = 0
                                    _res[f"has_{_sfx}"] = 0
                            for c in ["avg_1w", "avg_2w", "avg_4w"]:
                                _res[c] = _res[c].fillna(0)
                            for c in ["has_1w", "has_2w", "has_4w"]:
                                _res[c] = _res[c].fillna(0).astype(int)
                            def _wavg(r):
                                active = {k: w for k, w in _weights.items() if r[f"has_{k}"] > 0}
                                if not active:
                                    return 0.0
                                tw = sum(active.values())
                                return sum(r[f"avg_{k}"] * (w / tw) for k, w in active.items())
                            _res["weighted_kg"] = _res.apply(_wavg, axis=1)
                            _usage_for_report = _res[_res["weighted_kg"] > 0][["meat_name", "meat_origin", "weighted_kg"]]
            except Exception:
                _usage_for_report = None

            inv_fig = _build_inventory_image(saved_products, filtered_meats, inv_base_date, usage_df=_usage_for_report)
            st.pyplot(inv_fig)

            inv_buf = BytesIO()
            inv_fig.savefig(inv_buf, format="png", dpi=200, bbox_inches="tight",
                            facecolor="white", edgecolor="none")
            inv_buf.seek(0)
            plt.close(inv_fig)

            st.download_button(
                label="📥 이미지 다운로드 (PNG)",
                data=inv_buf.getvalue(),
                file_name=f"재고현황_{inv_base_date or date.today()}.png",
                mime="image/png",
                key="inv_report_img_download",
            )

        # ── 제품 상세 ──
        elif inv_sub == "📦 제품 상세":
            if saved_products.empty:
                st.info("제품 재고 데이터가 없습니다.")
            else:
                st.markdown("#### 제품 재고 현황")
                search_p = st.text_input("🔍 검색", placeholder="제품코드 또는 제품명...", key="inv_prod_search")
                disp = saved_products.copy()
                if search_p:
                    mask = (
                        disp["product_code"].astype(str).str.contains(search_p, case=False, na=False) |
                        disp["product_name"].astype(str).str.contains(search_p, case=False, na=False)
                    )
                    disp = disp[mask]
                show_df = disp[["product_code", "product_name", "remaining_box"]].rename(columns={
                    "product_code": "제품코드", "product_name": "제품명", "remaining_box": "현재고",
                })

                def _hl_zero(val):
                    return "color: red; font-weight: bold" if val == 0 else ""

                st.dataframe(
                    show_df.style.applymap(_hl_zero, subset=["현재고"]),
                    use_container_width=True, hide_index=True, height=600,
                )

        # ── 원육 상세 ──
        elif inv_sub == "🥩 원육 상세":
            if saved_meats.empty:
                st.info("원육 재고 데이터가 없습니다.")
            else:
                st.markdown("#### 원육 재고 현황")
                search_m = st.text_input("🔍 검색", placeholder="원육코드 또는 원육명...", key="inv_meat_search")
                disp_m = saved_meats.copy()
                if search_m:
                    mask = (
                        disp_m["meat_code"].astype(str).str.contains(search_m, case=False, na=False) |
                        disp_m["meat_name"].astype(str).str.contains(search_m, case=False, na=False)
                    )
                    disp_m = disp_m[mask]
                disp_m = disp_m.sort_values("meat_name", ascending=False, key=lambda s: s.str.lower()).reset_index(drop=True)
                show_m = disp_m[["meat_code", "meat_name", "origin", "remaining_kg", "remaining_box"]].copy()
                show_m.columns = ["원육코드", "원육명", "원산지", "kg", "박스"]
                st.dataframe(
                    show_m,
                    use_container_width=True, hide_index=True, height=600,
                    column_config={
                        "kg": st.column_config.NumberColumn("kg", format="%.2f"),
                    },
                )
