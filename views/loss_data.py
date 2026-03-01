import streamlit as st
import pandas as pd
from datetime import date
from supabase import create_client
from views.sales import (
    supabase,
    load_loss_assignments,
    sync_product_rawmeats,
    upsert_product_rawmeat,
)

# ========================
# DB 함수 (production_status)
# ========================

@st.cache_data(ttl=120)
def load_production_status_uploads():
    """업로드 배치 목록 조회"""
    try:
        result = supabase.table("production_status_uploads").select("*").order("upload_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["id", "upload_date", "file_name", "total_groups",
                                  "total_input_kg", "total_output_kg", "total_loss_kg"])


@st.cache_data(ttl=120)
def load_production_status_groups(upload_id=None):
    """그룹 목록 조회"""
    try:
        query = supabase.table("production_status_groups").select("*").order("group_index")
        if upload_id:
            query = query.eq("upload_id", upload_id)
        result = query.execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["id", "upload_id", "group_index", "total_input_kg",
                                  "total_output_kg", "loss_kg", "loss_rate",
                                  "total_input_amount", "total_output_amount"])


@st.cache_data(ttl=120)
def load_production_status_items(group_id=None):
    """항목 목록 조회"""
    try:
        query = supabase.table("production_status_items").select("*").order("id")
        if group_id:
            query = query.eq("group_id", group_id)
        result = query.execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame()


def _clear_production_status_caches():
    """캐시 클리어"""
    load_production_status_uploads.clear()
    load_production_status_groups.clear()
    load_production_status_items.clear()


def insert_production_status(upload_data, groups_with_items):
    """
    생산현황 데이터 일괄 저장.
    upload_data: dict (upload_date, file_name, total_groups, total_input_kg, total_output_kg, total_loss_kg)
    groups_with_items: list of dict, each with:
        group_data: dict (group_index, total_input_kg, total_output_kg, loss_kg, loss_rate, ...)
        items: list of dict (item rows)
    """
    # 1. 업로드 배치 생성
    upload_result = supabase.table("production_status_uploads").insert(upload_data).execute()
    upload_id = upload_result.data[0]["id"]

    # 2. 그룹별 저장
    for group_info in groups_with_items:
        group_data = group_info["group_data"].copy()
        group_data["upload_id"] = upload_id

        group_result = supabase.table("production_status_groups").insert(group_data).execute()
        group_id = group_result.data[0]["id"]

        # 3. 항목 저장
        items = group_info["items"]
        if items:
            for item in items:
                item["group_id"] = group_id
            # 500건씩 나눠 저장
            chunk_size = 500
            for i in range(0, len(items), chunk_size):
                chunk = items[i:i + chunk_size]
                supabase.table("production_status_items").insert(chunk).execute()

    _clear_production_status_caches()
    return upload_id


def delete_production_status_upload(upload_id):
    """업로드 배치 삭제 (CASCADE로 groups, items 자동 삭제)"""
    supabase.table("production_status_uploads").delete().eq("id", upload_id).execute()
    _clear_production_status_caches()


# ========================
# uploaded_products 조회 (로스 계산용)
# ========================

@st.cache_data(ttl=120)
def _load_uploaded_products_for_loss():
    """uploaded_products에서 박스당kg 조회"""
    try:
        result = supabase.table("uploaded_products").select("product_code, product_name, kg_per_box").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["product_code", "product_name", "kg_per_box"])


# ========================
# 엑셀 파싱 및 제품별 분리 로직
# ========================

def parse_production_excel(df_raw):
    """
    투입상품 기준 생산현황 엑셀을 파싱하여 제품별로 분리.

    규칙:
    - 원육O 상품O: 새 제품 + 새 원육
    - 원육O 상품X: 위 제품에 추가 원육 (두 가지 원육 사용)
    - 원육X 상품O: 새 제품, 위의 원육 사용 (상속)
    - 원육X 상품X: 빈 행 (컨텍스트 리셋)

    반환: list of product entries
    각 entry = {"product": {...}, "raw_meats": [{...}, ...]}
    """
    products = []
    last_meat = None
    last_product_entry = None

    for idx in range(len(df_raw)):
        row = df_raw.iloc[idx]

        meat_data = _extract_meat_data(row)
        product_data = _extract_product_data(row)

        has_meat = bool(str(meat_data.get("meat_code", "")).strip() or str(meat_data.get("meat_name", "")).strip())
        has_product = bool(str(product_data.get("product_code", "")).strip() or str(product_data.get("product_name", "")).strip())

        # 빈 행 - 컨텍스트 리셋
        if not has_meat and not has_product:
            last_meat = None
            last_product_entry = None
            continue

        # 원육코드와 상품코드가 같으면 제외
        if has_meat and has_product:
            m_code = str(meat_data.get("meat_code", "")).strip()
            p_code = str(product_data.get("product_code", "")).strip()
            if m_code and p_code and m_code == p_code:
                continue

        if has_meat and has_product:
            # 새 제품 + 새 원육
            product_data["expected_sales_amount"] = _safe_float(row, 16)
            product_data["expected_profit_amount"] = _safe_float(row, 17)
            entry = {"product": product_data, "raw_meats": [meat_data]}
            products.append(entry)
            last_meat = meat_data
            last_product_entry = entry

        elif has_meat and not has_product:
            # 위 제품에 추가 원육
            if last_product_entry:
                last_product_entry["raw_meats"].append(meat_data)
            last_meat = meat_data

        elif not has_meat and has_product:
            # 새 제품, 위의 원육 사용 (상속)
            product_data["expected_sales_amount"] = _safe_float(row, 16)
            product_data["expected_profit_amount"] = _safe_float(row, 17)
            inherited_meats = []
            if last_meat:
                inherited = last_meat.copy()
                inherited["_inherited"] = True
                inherited["meat_kg"] = 0.0
                inherited["meat_amount"] = 0.0
                inherited["meat_boxes"] = 0.0
                inherited_meats = [inherited]
            entry = {"product": product_data, "raw_meats": inherited_meats}
            products.append(entry)
            last_product_entry = entry

    return products


def _safe_float(row, col_idx):
    """안전하게 float 변환"""
    try:
        if col_idx < len(row):
            v = row.iloc[col_idx]
            if pd.notna(v):
                return float(v)
    except (ValueError, TypeError):
        pass
    return 0.0


def _safe_str(row, col_idx):
    """안전하게 string 변환"""
    try:
        if col_idx < len(row):
            v = row.iloc[col_idx]
            if pd.notna(v):
                return str(v).strip()
    except:
        pass
    return ""


def _extract_meat_data(row):
    """원육 데이터 추출 (컬럼 0-7)"""
    return {
        "meat_code": _safe_str(row, 0),
        "meat_name": _safe_str(row, 1),
        "meat_origin": _safe_str(row, 2),
        "meat_grade": _safe_str(row, 3),
        "meat_boxes": _safe_float(row, 4),
        "meat_kg": _safe_float(row, 5),
        "meat_unit": _safe_str(row, 6),
        "meat_amount": _safe_float(row, 7),
    }


def _extract_product_data(row):
    """상품 데이터 추출 (컬럼 8-15)"""
    return {
        "product_code": _safe_str(row, 8),
        "product_name": _safe_str(row, 9),
        "product_origin": _safe_str(row, 10),
        "product_grade": _safe_str(row, 11),
        "product_boxes": _safe_float(row, 12),
        "product_kg": _safe_float(row, 13),
        "product_unit": _safe_str(row, 14),
        "product_amount": _safe_float(row, 15),
    }


def calculate_product_loss(product_entry, uploaded_products_df):
    """
    제품별 로스 계산.
    원육: 중량(Kg) 합계
    상품: Box × 박스당kg (uploaded_products 테이블 참조)
    """
    total_input_kg = sum(m["meat_kg"] for m in product_entry["raw_meats"])
    total_input_amount = sum(m["meat_amount"] for m in product_entry["raw_meats"])

    prod = product_entry["product"]
    total_output_kg = 0.0
    total_output_amount = prod.get("product_amount", 0.0)

    # 상품은 항상 Box × 박스당kg으로 계산
    if prod["product_boxes"] > 0 and not uploaded_products_df.empty:
        match = uploaded_products_df[
            uploaded_products_df["product_code"] == prod["product_code"]
        ]
        if not match.empty:
            kg_per_box = float(match.iloc[0].get("kg_per_box", 0))
            total_output_kg = prod["product_boxes"] * kg_per_box

    loss_kg = total_input_kg - total_output_kg
    loss_rate = round((loss_kg / total_input_kg * 100), 2) if total_input_kg > 0 else 0

    return {
        "total_input_kg": round(total_input_kg, 2),
        "total_output_kg": round(total_output_kg, 2),
        "loss_kg": round(loss_kg, 2),
        "loss_rate": loss_rate,
        "total_input_amount": round(total_input_amount, 2),
        "total_output_amount": round(total_output_amount, 2),
    }


def sync_rawmeats_from_production_status(product_entries):
    """생산현황 업로드 후 product_rawmeats 동기화"""
    for entry in product_entries:
        product = entry["product"]
        p_name = str(product.get("product_name", "")).strip()
        if not p_name:
            continue
        for meat in entry["raw_meats"]:
            m_code = str(meat.get("meat_code", "")).strip()
            m_name = str(meat.get("meat_name", "")).strip()
            m_origin = str(meat.get("meat_origin", "")).strip()
            m_grade = str(meat.get("meat_grade", "")).strip()
            origin_grade = f"{m_origin} {m_grade}".strip() if m_origin or m_grade else ""
            if m_code or m_name:
                upsert_product_rawmeat(p_name, m_code, m_name, origin_grade)


# ========================
# 페이지 렌더링
# ========================

st.title("📉 로스 데이터")
st.caption("투입상품 기준 생산현황 업로드 및 로스 관리")

tab1, tab2 = st.tabs(["📋 투입상품 기준 생산현황", "📊 로스 현황"])

# ========================
# Tab 1: 투입상품 기준 생산현황
# ========================

with tab1:
    menu = st.radio("선택", [
        "📤 엑셀 업로드",
        "📋 업로드 이력",
    ], horizontal=True, key="production_status_menu")

    st.divider()

    # 성공 메시지
    for msg_key in ["_ps_upload_success", "_ps_delete_success"]:
        if st.session_state.get(msg_key):
            st.success(st.session_state[msg_key])
            del st.session_state[msg_key]

    # ── 엑셀 업로드 ──
    if menu == "📤 엑셀 업로드":
        st.subheader("📤 투입상품 기준 생산현황 업로드")
        st.caption("엑셀 파일을 업로드하면 제품별로 분리하고 로스를 자동 계산합니다.")

        st.markdown("""
        **엑셀 구조** (헤더 행 포함)

        - 원육O 상품O = 새 제품 + 새 원육
        - 원육O 상품X = 위 제품에 추가 원육 (두 가지 원육 사용)
        - 원육X 상품O = 새 제품, 위 원육 사용
        - 빈 행 = 구분자
        """)

        uploaded_file = st.file_uploader(
            "엑셀 파일 업로드 (.xlsx)",
            type=["xlsx", "xls"],
            key="production_status_upload_file"
        )

        if uploaded_file:
            try:
                df_raw = pd.read_excel(uploaded_file, header=0)

                if df_raw.empty:
                    st.warning("데이터가 없습니다.")
                else:
                    # 제품별 파싱
                    product_entries = parse_production_excel(df_raw)

                    if not product_entries:
                        st.warning("유효한 제품이 없습니다. 엑셀 형식을 확인해주세요.")
                    else:
                        uploaded_prod_df = _load_uploaded_products_for_loss()

                        # 제품별 로스 계산 (공유 원육: 처음 투입된 총키로수 기준)
                        products_with_loss = []
                        remaining_kg = 0.0
                        remaining_amount = 0.0
                        chain_original_input_kg = 0.0
                        chain_original_input_amount = 0.0
                        chain_total_output_kg = 0.0
                        chain_total_output_amount = 0.0

                        for i, entry in enumerate(product_entries):
                            has_inherited = any(m.get("_inherited") for m in entry["raw_meats"])

                            if has_inherited:
                                # 이전 제품에서 남은 kg를 투입량으로 사용
                                carry_kg = max(remaining_kg, 0)
                                carry_amount = max(remaining_amount, 0)
                                for m in entry["raw_meats"]:
                                    if m.get("_inherited"):
                                        m["meat_kg"] = carry_kg
                                        m["meat_amount"] = carry_amount
                            else:
                                # 새 체인 시작 - 처음 투입된 총키로수 기록
                                chain_original_input_kg = sum(m["meat_kg"] for m in entry["raw_meats"])
                                chain_original_input_amount = sum(m["meat_amount"] for m in entry["raw_meats"])
                                chain_total_output_kg = 0.0
                                chain_total_output_amount = 0.0

                            loss_info = calculate_product_loss(entry, uploaded_prod_df)

                            # 체인 누적 산출량 추적
                            chain_total_output_kg += loss_info["total_output_kg"]
                            chain_total_output_amount += loss_info["total_output_amount"]

                            # 다음 공유 제품을 위해 남은 kg 저장
                            remaining_kg = loss_info["loss_kg"]
                            remaining_amount = loss_info["total_input_amount"] - loss_info["total_output_amount"]

                            products_with_loss.append({
                                "entry": entry,
                                "loss_info": loss_info,
                                "index": i,
                                "chain_original_input_kg": chain_original_input_kg,
                                "chain_original_input_amount": chain_original_input_amount,
                                "chain_total_output_kg": chain_total_output_kg,
                                "chain_total_output_amount": chain_total_output_amount,
                            })

                        # 공유 원육 체인: 다음 제품이 상속이면 현재 제품 로스 0 (마지막만 로스 표시)
                        for idx in range(len(products_with_loss) - 1):
                            nxt = products_with_loss[idx + 1]["entry"]
                            nxt_inherited = any(m.get("_inherited") for m in nxt["raw_meats"])
                            if nxt_inherited:
                                products_with_loss[idx]["loss_info"]["loss_kg"] = 0
                                products_with_loss[idx]["loss_info"]["loss_rate"] = 0

                        # 공유 체인 마지막 제품: 처음 투입된 총키로수 기준으로 로스 재계산
                        for idx in range(len(products_with_loss)):
                            pinfo = products_with_loss[idx]
                            has_inherited = any(m.get("_inherited") for m in pinfo["entry"]["raw_meats"])
                            if not has_inherited:
                                continue
                            # 다음이 상속이 아니면 이 제품이 체인 마지막
                            is_last_in_chain = True
                            if idx + 1 < len(products_with_loss):
                                nxt = products_with_loss[idx + 1]["entry"]
                                if any(m.get("_inherited") for m in nxt["raw_meats"]):
                                    is_last_in_chain = False
                            if is_last_in_chain:
                                orig_input = pinfo["chain_original_input_kg"]
                                total_out = pinfo["chain_total_output_kg"]
                                loss_kg = round(orig_input - total_out, 2)
                                loss_rate = round((loss_kg / orig_input * 100), 2) if orig_input > 0 else 0
                                pinfo["loss_info"]["total_input_kg"] = round(orig_input, 2)
                                pinfo["loss_info"]["loss_kg"] = loss_kg
                                pinfo["loss_info"]["loss_rate"] = loss_rate
                                pinfo["loss_info"]["total_input_amount"] = round(pinfo["chain_original_input_amount"], 2)

                        # 전체 요약 (상속된 원육은 중복 계산하지 않음)
                        unique_input_kg = 0.0
                        for pinfo in products_with_loss:
                            for m in pinfo["entry"]["raw_meats"]:
                                if not m.get("_inherited"):
                                    unique_input_kg += m["meat_kg"]
                        total_output_kg = sum(p["loss_info"]["total_output_kg"] for p in products_with_loss)
                        total_loss_kg = unique_input_kg - total_output_kg
                        overall_loss_rate = round((total_loss_kg / unique_input_kg * 100), 2) if unique_input_kg > 0 else 0

                        st.success(f"총 **{len(product_entries)}개** 제품 파싱 완료")

                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("총 투입량", f"{unique_input_kg:,.1f}kg")
                        with col2:
                            st.metric("총 생산량", f"{total_output_kg:,.1f}kg")
                        with col3:
                            st.metric("총 로스", f"{total_loss_kg:,.1f}kg")
                        with col4:
                            st.metric("평균 로스율", f"{overall_loss_rate:.1f}%")

                        st.divider()

                        # 제품별 요약 테이블
                        summary_rows = []
                        for pinfo in products_with_loss:
                            entry = pinfo["entry"]
                            loss = pinfo["loss_info"]
                            prod = entry["product"]
                            meat_names = ", ".join([m["meat_name"] for m in entry["raw_meats"] if m["meat_name"]])
                            has_inherited = any(m.get("_inherited") for m in entry["raw_meats"])
                            if has_inherited:
                                meat_names += " (공유)"
                            summary_rows.append({
                                "상품코드": prod["product_code"],
                                "상품명": prod["product_name"],
                                "Box": prod["product_boxes"],
                                "생산(kg)": loss["total_output_kg"],
                                "원육명": meat_names,
                                "투입(kg)": loss["total_input_kg"],
                                "로스(kg)": loss["loss_kg"],
                                "로스율(%)": loss["loss_rate"],
                            })

                        st.dataframe(
                            pd.DataFrame(summary_rows).style.format({
                                "Box": "{:,.0f}",
                                "생산(kg)": "{:,.1f}",
                                "투입(kg)": "{:,.1f}",
                                "로스(kg)": "{:,.1f}",
                                "로스율(%)": "{:.1f}",
                            }),
                            use_container_width=True, hide_index=True
                        )

                        st.divider()

                        # 제품별 상세 (확장 가능)
                        for pinfo in products_with_loss:
                            entry = pinfo["entry"]
                            loss = pinfo["loss_info"]
                            prod = entry["product"]
                            idx = pinfo["index"]

                            label = f"제품 {idx + 1}: {prod['product_name']}"
                            if loss["loss_rate"] < 0:
                                label += f" (생산초과 {loss['loss_rate']:.1f}%)"
                            else:
                                label += f" (로스 {loss['loss_rate']:.1f}%)"

                            with st.expander(label, expanded=False):
                                if entry["raw_meats"]:
                                    st.markdown("**투입 원육**")
                                    meat_display = []
                                    for m in entry["raw_meats"]:
                                        row_data = {
                                            "원육코드": m["meat_code"],
                                            "원육명": m["meat_name"],
                                            "원산지": m["meat_origin"],
                                            "등급": m["meat_grade"],
                                            "Box": m["meat_boxes"],
                                            "중량(Kg)": m["meat_kg"],
                                            "금액": m["meat_amount"],
                                        }
                                        if m.get("_inherited"):
                                            row_data["비고"] = "공유"
                                        else:
                                            row_data["비고"] = ""
                                        meat_display.append(row_data)
                                    st.dataframe(pd.DataFrame(meat_display), use_container_width=True, hide_index=True)

                                st.markdown("**생산 상품**")
                                st.dataframe(pd.DataFrame([{
                                    "상품코드": prod["product_code"],
                                    "상품명": prod["product_name"],
                                    "원산지": prod["product_origin"],
                                    "등급": prod["product_grade"],
                                    "Box": prod["product_boxes"],
                                    "중량(Kg)": prod["product_kg"],
                                    "금액": prod["product_amount"],
                                }]), use_container_width=True, hide_index=True)

                                st.info(
                                    f"투입: **{loss['total_input_kg']:,.1f}kg** "
                                    f"({loss['total_input_amount']:,.0f}원) → "
                                    f"생산: **{loss['total_output_kg']:,.1f}kg** "
                                    f"({loss['total_output_amount']:,.0f}원) → "
                                    f"로스: **{loss['loss_kg']:,.1f}kg** "
                                    f"(**{loss['loss_rate']:.1f}%**)"
                                )

                        st.divider()

                        # 저장 버튼
                        if st.button("💾 업로드 확정 및 저장", type="primary", use_container_width=True,
                                     key="ps_upload_confirm"):
                            try:
                                upload_data = {
                                    "upload_date": date.today().strftime("%Y-%m-%d"),
                                    "file_name": uploaded_file.name,
                                    "total_groups": len(product_entries),
                                    "total_input_kg": round(unique_input_kg, 2),
                                    "total_output_kg": round(total_output_kg, 2),
                                    "total_loss_kg": round(total_loss_kg, 2),
                                }

                                save_groups = []
                                for pinfo in products_with_loss:
                                    entry = pinfo["entry"]
                                    loss = pinfo["loss_info"]

                                    group_data = {
                                        "group_index": pinfo["index"],
                                        "total_input_kg": loss["total_input_kg"],
                                        "total_output_kg": loss["total_output_kg"],
                                        "loss_kg": loss["loss_kg"],
                                        "loss_rate": loss["loss_rate"],
                                        "total_input_amount": loss["total_input_amount"],
                                        "total_output_amount": loss["total_output_amount"],
                                    }

                                    items = []
                                    for m in entry["raw_meats"]:
                                        items.append({
                                            "item_type": "raw_meat",
                                            "meat_code": m["meat_code"],
                                            "meat_name": m["meat_name"],
                                            "meat_origin": m["meat_origin"],
                                            "meat_grade": m["meat_grade"],
                                            "meat_boxes": m["meat_boxes"],
                                            "meat_kg": m["meat_kg"],
                                            "meat_unit": m["meat_unit"],
                                            "meat_amount": m["meat_amount"],
                                        })
                                    p = entry["product"]
                                    items.append({
                                        "item_type": "product",
                                        "product_code": p["product_code"],
                                        "product_name": p["product_name"],
                                        "product_origin": p["product_origin"],
                                        "product_grade": p["product_grade"],
                                        "product_boxes": p["product_boxes"],
                                        "product_kg": p["product_kg"],
                                        "product_unit": p["product_unit"],
                                        "product_amount": p["product_amount"],
                                        "expected_sales_amount": p.get("expected_sales_amount", 0),
                                        "expected_profit_amount": p.get("expected_profit_amount", 0),
                                    })

                                    save_groups.append({
                                        "group_data": group_data,
                                        "items": items,
                                    })

                                insert_production_status(upload_data, save_groups)

                                # product_rawmeats 동기화
                                sync_rawmeats_from_production_status(product_entries)

                                st.session_state["_ps_upload_success"] = f"✅ {len(product_entries)}개 제품 저장 완료!"
                                st.rerun()

                            except Exception as e:
                                st.error(f"❌ 저장 실패: {str(e)}")

            except Exception as e:
                st.error(f"❌ 파일 읽기 실패: {str(e)}")

    # ── 업로드 이력 ──
    elif menu == "📋 업로드 이력":
        st.subheader("📋 업로드 이력")

        uploads_df = load_production_status_uploads()

        if uploads_df.empty:
            st.info("업로드된 데이터가 없습니다.")
        else:
            # 요약 테이블
            display_data = []
            for _, row in uploads_df.iterrows():
                total_input = float(row.get("total_input_kg", 0) or 0)
                total_output = float(row.get("total_output_kg", 0) or 0)
                total_loss = float(row.get("total_loss_kg", 0) or 0)
                loss_rate = round((total_loss / total_input * 100), 1) if total_input > 0 else 0

                display_data.append({
                    "업로드일": row.get("upload_date", ""),
                    "파일명": row.get("file_name", ""),
                    "제품수": int(row.get("total_groups", 0) or 0),
                    "총투입(kg)": total_input,
                    "총생산(kg)": total_output,
                    "총로스(kg)": total_loss,
                    "로스율(%)": loss_rate,
                })

            summary_df = pd.DataFrame(display_data)
            st.dataframe(
                summary_df.style.format({
                    "총투입(kg)": "{:,.1f}",
                    "총생산(kg)": "{:,.1f}",
                    "총로스(kg)": "{:,.1f}",
                    "로스율(%)": "{:.1f}",
                }),
                use_container_width=True, hide_index=True
            )

            st.divider()

            # 상세 보기
            for _, upload_row in uploads_df.iterrows():
                uid = int(upload_row["id"])
                u_date = upload_row.get("upload_date", "")
                u_file = upload_row.get("file_name", "")
                u_prod_count = int(upload_row.get("total_groups", 0) or 0)

                with st.expander(f"📅 {u_date} - {u_file} ({u_prod_count}제품)", expanded=False):
                    groups_df = load_production_status_groups(uid)

                    if groups_df.empty:
                        st.info("데이터가 없습니다.")
                    else:
                        # 제품별 요약 테이블
                        prod_summary = []
                        for _, g_row in groups_df.iterrows():
                            gid = int(g_row["id"])
                            items_df = load_production_status_items(gid)

                            prod_name = ""
                            prod_code = ""
                            meat_names = ""
                            if not items_df.empty:
                                prods = items_df[items_df["item_type"] == "product"]
                                meats = items_df[items_df["item_type"] == "raw_meat"]
                                if not prods.empty:
                                    prod_name = str(prods.iloc[0].get("product_name", "") or "").strip()
                                    prod_code = str(prods.iloc[0].get("product_code", "") or "").strip()
                                meat_list = meats["meat_name"].dropna().astype(str).str.strip().tolist()
                                meat_names = ", ".join([n for n in meat_list if n])

                            prod_summary.append({
                                "상품코드": prod_code,
                                "상품명": prod_name,
                                "생산(kg)": float(g_row.get("total_output_kg", 0) or 0),
                                "원육명": meat_names,
                                "투입(kg)": float(g_row.get("total_input_kg", 0) or 0),
                                "로스(kg)": float(g_row.get("loss_kg", 0) or 0),
                                "로스율(%)": float(g_row.get("loss_rate", 0) or 0),
                            })

                        st.dataframe(
                            pd.DataFrame(prod_summary).style.format({
                                "생산(kg)": "{:,.1f}",
                                "투입(kg)": "{:,.1f}",
                                "로스(kg)": "{:,.1f}",
                                "로스율(%)": "{:.1f}",
                            }),
                            use_container_width=True, hide_index=True
                        )

                    # 삭제 버튼
                    if st.button(f"🗑️ 이 업로드 삭제", key=f"del_upload_{uid}"):
                        st.session_state[f"_confirm_del_{uid}"] = True

                    if st.session_state.get(f"_confirm_del_{uid}"):
                        st.warning("정말로 삭제하시겠습니까? 하위 데이터도 모두 삭제됩니다.")
                        c1, c2, _ = st.columns([1, 1, 4])
                        with c1:
                            if st.button("✅ 확인", key=f"confirm_del_{uid}"):
                                delete_production_status_upload(uid)
                                st.session_state[f"_confirm_del_{uid}"] = False
                                st.session_state["_ps_delete_success"] = "✅ 삭제 완료!"
                                st.rerun()
                        with c2:
                            if st.button("❌ 취소", key=f"cancel_del_{uid}"):
                                st.session_state[f"_confirm_del_{uid}"] = False
                                st.rerun()


# ========================
# Tab 2: 로스 현황 (읽기 전용)
# ========================

with tab2:
    st.subheader("📊 로스 현황")
    st.caption("투입상품 기준 생산현황 업로드 데이터에서 계산된 로스 현황입니다.")

    uploads_df = load_production_status_uploads()

    # 기존 loss_assignments 이력도 표시
    legacy_df = load_loss_assignments()
    has_legacy = not legacy_df.empty
    has_new = not uploads_df.empty

    if not has_new and not has_legacy:
        st.info("로스 데이터가 없습니다. '투입상품 기준 생산현황' 탭에서 엑셀을 업로드해주세요.")
    else:
        # ── 신규 데이터 (production_status) ──
        if has_new:
            st.markdown("#### 📋 생산현황 기반 로스")

            # 전체 요약
            total_input = uploads_df["total_input_kg"].fillna(0).astype(float).sum()
            total_output = uploads_df["total_output_kg"].fillna(0).astype(float).sum()
            total_loss = uploads_df["total_loss_kg"].fillna(0).astype(float).sum()
            avg_rate = round((total_loss / total_input * 100), 1) if total_input > 0 else 0

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("총 투입량", f"{total_input:,.1f}kg")
            with col2:
                st.metric("총 생산량", f"{total_output:,.1f}kg")
            with col3:
                st.metric("총 로스", f"{total_loss:,.1f}kg")
            with col4:
                st.metric("평균 로스율", f"{avg_rate:.1f}%")

            st.divider()

            # 업로드별 제품 요약
            for _, u_row in uploads_df.iterrows():
                uid = int(u_row["id"])
                u_date = u_row.get("upload_date", "")
                u_input = float(u_row.get("total_input_kg", 0) or 0)
                u_output = float(u_row.get("total_output_kg", 0) or 0)
                u_loss = float(u_row.get("total_loss_kg", 0) or 0)
                u_rate = round((u_loss / u_input * 100), 1) if u_input > 0 else 0

                groups_df = load_production_status_groups(uid)

                st.markdown(
                    f"**📅 {u_date}** — "
                    f"투입 {u_input:,.1f}kg → 생산 {u_output:,.1f}kg → "
                    f"로스 {u_loss:,.1f}kg ({u_rate:.1f}%)"
                )

                if not groups_df.empty:
                    g_display = []
                    for _, g_row in groups_df.iterrows():
                        # 로스율 0% 또는 100%는 로스 현황에서 제외
                        g_rate = float(g_row.get("loss_rate", 0) or 0)
                        if g_rate == 0 or g_rate >= 100:
                            continue

                        gid = int(g_row["id"])
                        items_df = load_production_status_items(gid)

                        prod_name = ""
                        prod_code = ""
                        meat_names = ""
                        if not items_df.empty:
                            prods = items_df[items_df["item_type"] == "product"]
                            meats = items_df[items_df["item_type"] == "raw_meat"]
                            if not prods.empty:
                                prod_name = str(prods.iloc[0].get("product_name", "") or "").strip()
                                prod_code = str(prods.iloc[0].get("product_code", "") or "").strip()
                            meat_list = meats["meat_name"].dropna().astype(str).str.strip().tolist()
                            meat_names = ", ".join([n for n in meat_list if n])

                        g_display.append({
                            "상품코드": prod_code,
                            "상품명": prod_name,
                            "원육": meat_names,
                            "투입(kg)": float(g_row.get("total_input_kg", 0) or 0),
                            "생산(kg)": float(g_row.get("total_output_kg", 0) or 0),
                            "로스(kg)": float(g_row.get("loss_kg", 0) or 0),
                            "로스율(%)": g_rate,
                        })

                    if g_display:
                        st.dataframe(
                            pd.DataFrame(g_display).style.format({
                                "투입(kg)": "{:,.1f}",
                                "생산(kg)": "{:,.1f}",
                                "로스(kg)": "{:,.1f}",
                                "로스율(%)": "{:.1f}",
                            }),
                            use_container_width=True, hide_index=True
                        )

                st.divider()

        # ── 기존 데이터 (loss_assignments) ──
        if has_legacy:
            completed = legacy_df[
                (legacy_df["completed"] == True) &
                (legacy_df["product_name"].fillna("").astype(str).str.strip() != "")
            ]

            if not completed.empty:
                st.markdown("#### 📋 기존 로스 할당 이력")
                st.caption("이전 방식(수동 할당)으로 기록된 로스 데이터입니다.")

                summary_data = []
                for _, row in completed.iterrows():
                    kg = float(row.get("kg", 0) or 0)
                    prod_kg = float(row.get("production_kg", 0) or 0)
                    loss_kg = round(kg - prod_kg, 2) if kg > 0 and prod_kg > 0 else 0
                    loss_rate = round(loss_kg / kg * 100, 2) if kg > 0 and prod_kg > 0 else None

                    summary_data.append({
                        "날짜": row.get("move_date", ""),
                        "제품명": row.get("product_name", ""),
                        "원육명": row.get("meat_name", ""),
                        "투입(kg)": kg,
                        "생산(kg)": prod_kg,
                        "로스(kg)": loss_kg,
                        "로스율(%)": loss_rate,
                    })

                legacy_summary = pd.DataFrame(summary_data)
                st.dataframe(
                    legacy_summary.style.format({
                        "투입(kg)": "{:,.1f}",
                        "생산(kg)": "{:,.1f}",
                        "로스(kg)": "{:,.1f}",
                        "로스율(%)": "{:.1f}",
                    }, na_rep="-"),
                    use_container_width=True, hide_index=True
                )

                # 기존 로스 이력 삭제
                st.divider()

                # 날짜별 삭제
                legacy_dates = sorted(completed["move_date"].dropna().unique().tolist(), reverse=True)
                del_dates = st.multiselect(
                    "🗑️ 삭제할 날짜 선택",
                    options=legacy_dates,
                    placeholder="날짜를 선택하세요",
                    key="legacy_loss_del_dates"
                )

                del_all = st.checkbox("전체 기존 이력 삭제", key="legacy_loss_del_all")

                if del_dates or del_all:
                    if del_all:
                        target_ids = legacy_df["id"].tolist()
                        target_count = len(legacy_df)
                    else:
                        target = legacy_df[legacy_df["move_date"].isin(del_dates)]
                        target_ids = target["id"].tolist()
                        target_count = len(target)

                    st.warning(f"⚠️ {target_count}건의 기존 로스 이력이 삭제됩니다.")

                    if st.button("🗑️ 삭제", type="primary", key="legacy_loss_del_btn"):
                        st.session_state["_confirm_legacy_del"] = True

                    if st.session_state.get("_confirm_legacy_del"):
                        st.error(f"정말로 {target_count}건을 삭제하시겠습니까? 복구할 수 없습니다.")
                        c1, c2, _ = st.columns([1, 1, 4])
                        with c1:
                            if st.button("✅ 확인", key="confirm_legacy_del"):
                                try:
                                    from views.sales import delete_loss_assignment
                                    for rid in target_ids:
                                        delete_loss_assignment(int(rid))
                                    sync_product_rawmeats()
                                    st.session_state["_confirm_legacy_del"] = False
                                    st.success(f"✅ {target_count}건 삭제 완료!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 삭제 실패: {str(e)}")
                        with c2:
                            if st.button("❌ 취소", key="cancel_legacy_del"):
                                st.session_state["_confirm_legacy_del"] = False
                                st.rerun()
