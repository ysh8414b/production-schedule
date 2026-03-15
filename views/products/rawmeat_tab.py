import streamlit as st
import pandas as pd
from views.products import supabase, load_products
from utils.auth import is_authenticated, can_edit


# ========================
# 원육 DB 함수
# ========================

def load_raw_meats():
    """raw_meats 테이블에서 원육 목록 로드 (테이블이 없으면 products에서 추출)"""
    try:
        result = supabase.table("raw_meats").select("*").order("name").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["id", "name", "category", "origin", "memo"])


def check_duplicate_raw_meat(name, origin="", exclude_id=None):
    """원육명+원산지 조합이 이미 존재하는지 확인"""
    df = load_raw_meats()
    if df.empty:
        return False
    
    name_str = str(name).strip()
    origin_str = str(origin).strip() if origin else ""
    
    # 원산지가 있는 경우와 없는 경우 모두 체크
    if origin_str:
        matching = df[(df["name"] == name_str) & (df["origin"] == origin_str)]
    else:
        matching = df[(df["name"] == name_str) & (df["origin"].fillna("").astype(str).str.strip() == "")]
    
    # 수정 시에는 현재 항목 제외
    if exclude_id:
        matching = matching[matching["id"] != exclude_id]
    
    return not matching.empty


def upsert_raw_meat(name, category="", origin="", memo="", meat_id=None):
    """원육 등록/수정 (meat_id가 있으면 수정, 없으면 신규 등록)"""
    name_str = str(name).strip()
    origin_str = str(origin).strip() if origin else ""
    
    # 중복 체크 (신규 등록 시 또는 수정 시 원산지 변경 시)
    if check_duplicate_raw_meat(name_str, origin_str, exclude_id=meat_id):
        raise ValueError(f"이미 등록된 원육입니다: '{name_str}' (원산지: '{origin_str if origin_str else '없음'}')")
    
    data = {
        "name": name_str,
        "category": str(category).strip() if category else "",
        "origin": origin_str,
        "memo": str(memo).strip() if memo else ""
    }
    if meat_id:
        # 수정: id로 업데이트
        supabase.table("raw_meats").update(data).eq("id", meat_id).execute()
    else:
        # 신규: 항상 insert
        supabase.table("raw_meats").insert(data).execute()


def delete_raw_meat(meat_id):
    supabase.table("raw_meats").delete().eq("id", meat_id).execute()


# ========================
# 렌더링
# ========================

def render_rawmeat_tab():
    """원육 관리 탭"""

    menu_options = ["📋 원육 목록"]
    if can_edit("products"):
        menu_options.append("✏️ 원육 등록/수정")
    menu_options.append("📊 원육별 제품 현황")

    menu = st.radio("선택", menu_options, horizontal=True, key="rawmeat_menu")

    st.divider()

    if menu == "📋 원육 목록":
        _show_rawmeat_list()
    elif menu == "✏️ 원육 등록/수정":
        _show_rawmeat_form()
    elif menu == "📊 원육별 제품 현황":
        _show_rawmeat_products()


def _show_rawmeat_list():
    st.subheader("등록된 원육 목록")

    df = load_raw_meats()

    if df.empty:
        st.info("등록된 원육이 없습니다. '원육 등록/수정'에서 추가해주세요.")

        # products 테이블에서 사용 중인 원육 자동 추출 제안
        if can_edit("products"):
            products_df = load_products()
            if not products_df.empty:
                meats = products_df["used_raw_meat"].fillna("").astype(str).str.strip()
                unique_meats = sorted(meats[meats != ""].unique().tolist())
                if unique_meats:
                    st.divider()
                    st.caption(f"💡 현재 제품에서 사용 중인 원육: **{', '.join(unique_meats)}**")
                    if st.button("🔄 제품에서 사용 중인 원육 자동 등록", key="auto_import_meats"):
                        success_count = 0
                        skipped_count = 0
                        for meat_name in unique_meats:
                            try:
                                # 중복 체크 후 등록 (원산지 없이)
                                if not check_duplicate_raw_meat(meat_name, ""):
                                    upsert_raw_meat(meat_name)
                                    success_count += 1
                                else:
                                    skipped_count += 1
                            except Exception:
                                skipped_count += 1
                        if success_count > 0:
                            st.session_state['rawmeat_auto_success'] = f"✅ {success_count}개 원육 등록 완료!"
                        if skipped_count > 0:
                            st.session_state['rawmeat_auto_info'] = f"ℹ️ {skipped_count}개 원육은 이미 등록되어 있어 건너뛰었습니다."
                        st.rerun()
    
    # 자동 등록 성공 메시지 표시
    if 'rawmeat_auto_success' in st.session_state:
        st.success(st.session_state['rawmeat_auto_success'])
        if hasattr(st, 'toast'):
            try:
                st.toast(st.session_state['rawmeat_auto_success'], icon="✅")
            except:
                pass
        del st.session_state['rawmeat_auto_success']
    if 'rawmeat_auto_info' in st.session_state:
        st.info(st.session_state['rawmeat_auto_info'])
        del st.session_state['rawmeat_auto_info']
        return

    # 메트릭
    col1, col2 = st.columns(2)
    with col1:
        st.metric("등록 원육 수", f"{len(df)}개")
    with col2:
        if "category" in df.columns:
            cats = df["category"].fillna("").astype(str).str.strip()
            st.metric("분류 수", f"{cats[cats != ''].nunique()}개")

    st.divider()

    # 테이블 표시
    display_cols = [c for c in ["name", "category", "origin", "memo"] if c in df.columns]
    col_names = {"name": "원육명", "category": "분류", "origin": "원산지", "memo": "메모"}
    display_df = df[display_cols].rename(columns=col_names)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # 삭제
    if can_edit("products"):
        st.divider()
        st.subheader("🗑️ 원육 삭제")
        # 같은 이름이 여러 개일 수 있으므로 원산지 정보도 함께 표시
        if "origin" in df.columns:
            delete_options = df.apply(
                lambda r: f"{r['name']} ({r['origin']})" if r.get('origin', '') else r['name'],
                axis=1
            ).tolist()
        else:
            delete_options = df["name"].tolist()
        delete_target = st.selectbox(
            "삭제할 원육 선택", options=delete_options, index=None,
            placeholder="원육을 선택하세요...", key="rawmeat_delete_target"
        )

        if delete_target:
            col_a, col_b = st.columns([1, 4])
            with col_a:
                if st.button("🗑️ 삭제", type="primary", key="rawmeat_delete_btn"):
                    # 선택된 옵션에서 원육명 추출
                    if "origin" in df.columns:
                        target_name = delete_target.split(" (")[0]
                        target_origin = delete_target.split(" (")[1].rstrip(")") if " (" in delete_target else ""
                        if target_origin:
                            meat_id = df[(df["name"] == target_name) & (df["origin"] == target_origin)]["id"].iloc[0]
                        else:
                            meat_id = df[df["name"] == target_name]["id"].iloc[0]
                    else:
                        meat_id = df[df["name"] == delete_target]["id"].iloc[0]
                    delete_raw_meat(meat_id)
                    st.session_state['rawmeat_delete_success'] = f"✅ '{delete_target}' 삭제 완료"
                    st.rerun()

    # 삭제 성공 메시지 표시
    if 'rawmeat_delete_success' in st.session_state:
        st.success(st.session_state['rawmeat_delete_success'])
        if hasattr(st, 'toast'):
            try:
                st.toast(st.session_state['rawmeat_delete_success'], icon="✅")
            except:
                pass
        del st.session_state['rawmeat_delete_success']


def _show_rawmeat_form():
    st.subheader("원육 등록 / 수정")
    st.caption("원육명은 중복 가능합니다. 원산지가 다르면 같은 이름으로 등록할 수 있습니다.")

    df = load_raw_meats()

    # 기존 원육 선택 옵션 (원산지 정보 포함)
    if not df.empty and "origin" in df.columns:
        existing_options = [""] + df.apply(
            lambda r: f"{r['name']} ({r['origin']})" if r.get('origin', '') else r['name'],
            axis=1
        ).tolist()
    elif not df.empty:
        existing_options = [""] + df["name"].tolist()
    else:
        existing_options = [""]
    
    existing = st.selectbox(
        "기존 원육 수정 (새 원육이면 비워두세요)",
        options=existing_options, index=0, key="rawmeat_existing_select"
    )

    # 선택된 원육의 정보 로드
    selected_meat_id = None
    if existing and not df.empty:
        if "origin" in df.columns and " (" in existing:
            target_name = existing.split(" (")[0]
            target_origin = existing.split(" (")[1].rstrip(")")
            matching = df[(df["name"] == target_name) & (df["origin"] == target_origin)]
            if not matching.empty:
                row = matching.iloc[0]
                selected_meat_id = row["id"]
                default_name = row["name"]
                default_cat = row.get("category", "") or ""
                default_origin = row.get("origin", "") or ""
                default_memo = row.get("memo", "") or ""
            else:
                default_name = ""
                default_cat = ""
                default_origin = ""
                default_memo = ""
        else:
            row = df[df["name"] == existing].iloc[0]
            selected_meat_id = row["id"]
            default_name = row["name"]
            default_cat = row.get("category", "") or ""
            default_origin = row.get("origin", "") or ""
            default_memo = row.get("memo", "") or ""
    else:
        default_name = ""
        default_cat = ""
        default_origin = ""
        default_memo = ""

    # 성공 메시지 표시 (폼 밖에서)
    if 'rawmeat_success_msg' in st.session_state:
        st.success(st.session_state['rawmeat_success_msg'])
        if hasattr(st, 'toast'):
            try:
                st.toast(st.session_state['rawmeat_success_msg'], icon="✅")
            except:
                pass
        del st.session_state['rawmeat_success_msg']
    
    with st.form("rawmeat_form"):
        name = st.text_input("원육명", value=default_name, placeholder="예: 등심, 안심, 삼겹살")

        col1, col2 = st.columns(2)
        with col1:
            category = st.text_input("분류", value=default_cat, placeholder="예: 소, 돼지, 닭")
        with col2:
            origin = st.text_input("원산지", value=default_origin, placeholder="예: 국내산, 호주산")

        memo = st.text_area("메모", value=default_memo, placeholder="추가 메모...", height=80)

        submitted = st.form_submit_button("💾 저장", type="primary")

        if submitted:
            if not name.strip():
                st.error("원육명을 입력해주세요.")
            else:
                try:
                    upsert_raw_meat(name.strip(), category, origin, memo, selected_meat_id)
                    if selected_meat_id:
                        st.session_state['rawmeat_success_msg'] = f"✅ '{name}' 수정 완료!"
                    else:
                        st.session_state['rawmeat_success_msg'] = f"✅ '{name}' 등록 완료!"
                    st.rerun()
                except ValueError as e:
                    st.error(f"❌ {str(e)}")
                except Exception as e:
                    st.error(f"❌ 저장 중 오류가 발생했습니다: {str(e)}")


def _show_rawmeat_products():
    st.subheader("📊 원육별 제품 현황")

    products_df = load_products()

    if products_df.empty:
        st.info("등록된 제품이 없습니다.")
        return

    meats = products_df["used_raw_meat"].fillna("").astype(str).str.strip()
    unique_meats = sorted(meats[meats != ""].unique().tolist())

    if not unique_meats:
        st.warning("사용원육이 등록된 제품이 없습니다.")
        return

    # 요약 메트릭
    st.metric("사용 중인 원육 종류", f"{len(unique_meats)}개")
    st.divider()

    for meat_name in unique_meats:
        meat_products = products_df[
            products_df["used_raw_meat"].fillna("").astype(str).str.strip() == meat_name
        ]
        with st.expander(f"🥩 {meat_name}  ({len(meat_products)}개 제품)", expanded=False):
            display_df = meat_products[["product_code", "product_name", "category"]].rename(columns={
                "product_code": "제품코드",
                "product_name": "제품명",
                "category": "분류"
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)

    # 미분류 제품
    no_meat = products_df[meats == ""]
    if not no_meat.empty:
        with st.expander(f"❓ 원육 미등록  ({len(no_meat)}개 제품)", expanded=False):
            display_df = no_meat[["product_code", "product_name", "category"]].rename(columns={
                "product_code": "제품코드",
                "product_name": "제품명",
                "category": "분류"
            })
            st.dataframe(display_df, use_container_width=True, hide_index=True)
