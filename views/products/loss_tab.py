import streamlit as st
import pandas as pd
from io import BytesIO
from views.products import supabase, load_products
from datetime import date, datetime
from utils.auth import is_authenticated, can_edit


# ========================
# 로스 DB 함수
# ========================

@st.cache_data(ttl=180)
def load_losses():
    """losses 테이블에서 로스 데이터 로드 (캐시 3분)"""
    try:
        result = supabase.table("losses").select("*").order("loss_date", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "loss_date", "product_code", "product_name",
        "weight_kg", "memo"
    ])


def get_product_code_by_name(product_name):
    """제품명으로 제품코드 조회"""
    try:
        products_df = load_products()
        if not products_df.empty:
            match = products_df[products_df["product_name"].astype(str).str.strip() == str(product_name).strip()]
            if not match.empty:
                return str(match.iloc[0].get("product_code", "")).strip()
    except:
        pass
    return ""


def get_raw_meat_by_name(product_name):
    """제품명으로 원육(사용원육) 조회"""
    try:
        products_df = load_products()
        if not products_df.empty:
            match = products_df[products_df["product_name"].astype(str).str.strip() == str(product_name).strip()]
            if not match.empty:
                return str(match.iloc[0].get("used_raw_meat", "")).strip()
    except:
        pass
    return ""


def insert_loss(loss_date, product_code, product_name, weight_kg, memo,
                brand="", tracking_number="", input_kg=0.0, output_kg=0.0, loss_rate=None, raw_meat=""):
    data = {
        "loss_date": str(loss_date),
        "product_code": str(product_code).strip(),
        "product_name": str(product_name).strip(),
        "weight_kg": float(weight_kg) if weight_kg else 0.0,
        "memo": str(memo).strip() if memo else "",
        "brand": str(brand).strip() if brand else "",
        "tracking_number": str(tracking_number).strip() if tracking_number else "",
        "input_kg": float(input_kg) if input_kg else 0.0,
        "output_kg": float(output_kg) if output_kg else 0.0,
        "raw_meat": str(raw_meat).strip() if raw_meat else "",
    }
    if loss_rate is not None:
        data["loss_rate"] = float(loss_rate)
    supabase.table("losses").insert(data).execute()
    _clear_loss_caches()


def delete_loss(loss_id):
    supabase.table("losses").delete().eq("id", loss_id).execute()
    _clear_loss_caches()


def update_loss(loss_id, data: dict):
    """losses 테이블의 특정 행 업데이트"""
    supabase.table("losses").update(data).eq("id", loss_id).execute()
    _clear_loss_caches()


def _clear_loss_caches():
    """로스 관련 캐시 일괄 클리어"""
    load_losses.clear()
    _prepare_loss_df.clear()


# ========================
# 생산기록 DB 함수
# ========================

@st.cache_data(ttl=180)
def load_production_records(week_start=None):
    """production_records 테이블에서 생산기록 로드 (캐시 3분)"""
    try:
        query = supabase.table("production_records").select("*")
        if week_start:
            query = query.eq("week_start", str(week_start))
        result = query.order("created_at", desc=True).execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=[
        "id", "week_start", "product", "quantity", "shift", "day_of_week",
        "input_kg", "output_kg", "brand", "tracking_number",
        "loss_rate", "completed", "completed_date", "created_at"
    ])


def save_production_record(record_data):
    """생산기록 저장 (upsert)"""
    supabase.table("production_records").upsert(
        record_data,
        on_conflict="id"
    ).execute()
    load_production_records.clear()


def insert_production_record(data):
    """생산기록 신규 등록"""
    supabase.table("production_records").insert(data).execute()
    load_production_records.clear()


def complete_production(record_id, input_kg, output_kg, brand, tracking_number):
    """생산 완료 처리 - 로스율 계산 후 업데이트"""
    loss_kg = input_kg - output_kg
    loss_rate = round((loss_kg / input_kg * 100), 2) if input_kg > 0 else 0.0
    today = date.today().strftime('%Y-%m-%d')

    supabase.table("production_records").update({
        "input_kg": float(input_kg),
        "output_kg": float(output_kg),
        "brand": str(brand).strip(),
        "tracking_number": str(tracking_number).strip(),
        "loss_rate": loss_rate,
        "completed": True,
        "completed_date": today
    }).eq("id", record_id).execute()
    load_production_records.clear()

    return loss_kg, loss_rate, today


def delete_production_record(record_id):
    supabase.table("production_records").delete().eq("id", record_id).execute()
    load_production_records.clear()


# ========================
# 스케줄 데이터 조회
# ========================

@st.cache_data(ttl=300)
def get_schedule_weeks():
    """schedules 테이블에서 주차 목록 조회 (캐시 5분)"""
    try:
        result = supabase.table("schedules").select(
            "week_start, week_end"
        ).order("week_start", desc=True).execute()
        if result.data:
            seen = set()
            weeks = []
            for row in result.data:
                key = (row["week_start"], row["week_end"])
                if key not in seen:
                    seen.add(key)
                    weeks.append(key)
            return weeks
    except:
        pass
    return []


@st.cache_data(ttl=300)
def load_schedule_products(week_start):
    """해당 주차의 스케줄 제품 목록 로드 (캐시 5분)"""
    try:
        result = supabase.table("schedules").select("*").eq(
            "week_start", str(week_start)
        ).order("id").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_brands_list():
    """brands 테이블에서 브랜드명 목록 로드 (캐시 5분)"""
    try:
        result = supabase.table("brands").select("name").order("name").execute()
        if result.data:
            return [row["name"] for row in result.data]
    except:
        pass
    return []


# ========================
# 엑셀 업로드
# ========================

def _show_report_download():
    st.subheader("📥 로스 보고서 출력")

    df = load_losses()

    if df.empty:
        st.info("등록된 로스 데이터가 없습니다.")
        return

    # 날짜 범위 선택
    st.markdown("#### 기간 선택")
    if "loss_date" in df.columns and df["loss_date"].notna().any():
        df["loss_date_dt"] = pd.to_datetime(df["loss_date"], errors="coerce")
        min_date = df["loss_date_dt"].min().date()
        max_date = df["loss_date_dt"].max().date()

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("시작일", value=min_date, min_value=min_date, max_value=max_date, key="report_start")
        with col2:
            end_date = st.date_input("종료일", value=max_date, min_value=min_date, max_value=max_date, key="report_end")

        # 기간 필터
        mask = (df["loss_date_dt"].dt.date >= start_date) & (df["loss_date_dt"].dt.date <= end_date)
        filtered = df[mask].copy()
    else:
        filtered = df.copy()
        start_date = date.today()
        end_date = date.today()

    st.caption(f"📊 선택 기간: {start_date} ~ {end_date} | {len(filtered)}건")

    if filtered.empty:
        st.warning("선택 기간에 해당하는 데이터가 없습니다.")
        return

    # 원육 정보 조인
    products_df = load_products()
    if not products_df.empty and "product_name" in filtered.columns:
        product_meat_map = dict(zip(
            products_df["product_name"].astype(str).str.strip(),
            products_df["used_raw_meat"].fillna("").astype(str).str.strip()
        ))
        filtered["raw_meat"] = filtered["product_name"].map(product_meat_map).fillna("")
    else:
        filtered["raw_meat"] = ""

    # 브랜드 추출
    def extract_brand(row):
        if pd.notna(row.get("brand")) and str(row.get("brand", "")).strip():
            return str(row["brand"]).strip()
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "브랜드:" in memo_str:
            try:
                return memo_str.split("브랜드:")[1].split("|")[0].strip()
            except:
                pass
        return ""
    filtered["brand_name"] = filtered.apply(extract_brand, axis=1)

    # 로스율 계산
    def calc_rate(row):
        if pd.notna(row.get("loss_rate")) and row.get("loss_rate") not in [None, 0, 0.0, ""]:
            return float(row["loss_rate"])
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "투입:" in memo_str and "생산:" in memo_str:
            try:
                inp = float(memo_str.split("투입:")[1].split("kg")[0].strip())
                out = float(memo_str.split("생산:")[1].split("kg")[0].strip())
                if inp > 0:
                    return round((inp - out) / inp * 100, 2)
            except:
                pass
        return None
    filtered["loss_rate_val"] = filtered.apply(calc_rate, axis=1)

    # 투입/생산 kg 추출
    def extract_kg(row, field):
        if pd.notna(row.get(f"{field}_kg")) and row.get(f"{field}_kg") not in [None, 0, 0.0]:
            return float(row[f"{field}_kg"])
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        label = "투입:" if field == "input" else "생산:"
        if label in memo_str:
            try:
                return float(memo_str.split(label)[1].split("kg")[0].strip())
            except:
                pass
        return None
    filtered["input_kg_val"] = filtered.apply(lambda r: extract_kg(r, "input"), axis=1)
    filtered["output_kg_val"] = filtered.apply(lambda r: extract_kg(r, "output"), axis=1)

    # 이력번호 추출
    def extract_tracking(row):
        if pd.notna(row.get("tracking_number")) and str(row.get("tracking_number", "")).strip():
            return str(row["tracking_number"]).strip()
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "이력번호:" in memo_str:
            try:
                return memo_str.split("이력번호:")[1].split("|")[0].strip()
            except:
                pass
        return ""
    filtered["tracking"] = filtered.apply(extract_tracking, axis=1)

    # ========== 엑셀 보고서 생성 ==========
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:

        # 시트1: 상세 데이터
        detail_df = filtered[["loss_date", "product_name", "raw_meat", "brand_name",
                               "tracking", "input_kg_val", "output_kg_val",
                               "weight_kg", "loss_rate_val", "memo"]].copy()
        detail_df = detail_df.rename(columns={
            "loss_date": "날짜", "product_name": "제품명", "raw_meat": "원육",
            "brand_name": "브랜드", "tracking": "이력번호",
            "input_kg_val": "투입(kg)", "output_kg_val": "생산(kg)",
            "weight_kg": "로스(kg)", "loss_rate_val": "로스율(%)", "memo": "메모"
        })
        detail_df.to_excel(writer, sheet_name="상세데이터", index=False)

        # 시트2: 제품별 요약
        product_summary = filtered.groupby("product_name").agg(
            생산건수=("id", "count"),
            총로스중량=("weight_kg", "sum")
        ).reset_index()
        rates_by_product = filtered[filtered["loss_rate_val"].notna()].groupby("product_name")["loss_rate_val"].mean().round(1)
        product_summary["평균로스율"] = product_summary["product_name"].map(rates_by_product).fillna("")
        product_summary["총로스중량"] = product_summary["총로스중량"].round(1)
        product_summary = product_summary.sort_values("총로스중량", ascending=False)
        product_summary = product_summary.rename(columns={
            "product_name": "제품명", "생산건수": "생산 건수",
            "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
        })
        product_summary.to_excel(writer, sheet_name="제품별요약", index=False)

        # 시트3: 원육별 요약
        meat_filtered = filtered[filtered["raw_meat"] != ""]
        if not meat_filtered.empty:
            meat_summary = meat_filtered.groupby("raw_meat").agg(
                생산건수=("id", "count"),
                총로스중량=("weight_kg", "sum")
            ).reset_index()
            meat_rates = meat_filtered[meat_filtered["loss_rate_val"].notna()].groupby("raw_meat")["loss_rate_val"].mean().round(1)
            meat_summary["평균로스율"] = meat_summary["raw_meat"].map(meat_rates).fillna("")
            meat_summary["총로스중량"] = meat_summary["총로스중량"].round(1)
            meat_summary = meat_summary.sort_values("총로스중량", ascending=False)
            meat_summary = meat_summary.rename(columns={
                "raw_meat": "원육", "생산건수": "생산 건수",
                "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
            })
            meat_summary.to_excel(writer, sheet_name="원육별요약", index=False)

        # 시트4: 일별 요약
        if "loss_date_dt" in filtered.columns:
            daily = filtered.groupby("loss_date").agg(
                생산건수=("id", "count"),
                총로스중량=("weight_kg", "sum")
            ).reset_index()
            daily_rates = filtered[filtered["loss_rate_val"].notna()].groupby("loss_date")["loss_rate_val"].mean().round(1)
            daily["평균로스율"] = daily["loss_date"].map(daily_rates).fillna("")
            daily["총로스중량"] = daily["총로스중량"].round(1)
            daily = daily.sort_values("loss_date")
            daily = daily.rename(columns={
                "loss_date": "날짜", "생산건수": "생산 건수",
                "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
            })
            daily.to_excel(writer, sheet_name="일별요약", index=False)

        # 시트5: 보고서 요약
        rates = filtered["loss_rate_val"].dropna()
        summary_data = {
            "항목": ["보고 기간", "총 건수", "총 로스 중량(kg)", "평균 로스율(%)", "최고 로스율(%)", "최저 로스율(%)", "생성일시"],
            "값": [
                f"{start_date} ~ {end_date}",
                str(len(filtered)),
                f"{filtered['weight_kg'].sum():,.1f}",
                f"{rates.mean():.1f}" if not rates.empty else "-",
                f"{rates.max():.1f}" if not rates.empty else "-",
                f"{rates.min():.1f}" if not rates.empty else "-",
                datetime.now().strftime("%Y-%m-%d %H:%M")
            ]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="보고서요약", index=False)

    # 미리보기
    st.divider()
    st.markdown("#### 미리보기")

    tab1, tab2, tab3 = st.tabs(["상세 데이터", "제품별 요약", "일별 요약"])
    with tab1:
        st.dataframe(detail_df.head(20), use_container_width=True, hide_index=True)
        if len(detail_df) > 20:
            st.caption(f"... 외 {len(detail_df) - 20}건")
    with tab2:
        st.dataframe(product_summary, use_container_width=True, hide_index=True)
    with tab3:
        if "loss_date_dt" in filtered.columns:
            st.dataframe(daily, use_container_width=True, hide_index=True)

    # 다운로드 버튼
    st.divider()
    filename = f"로스보고서_{start_date}_{end_date}.xlsx"

    st.download_button(
        label="📥 엑셀 보고서 다운로드",
        data=output.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        key="loss_report_download"
    )
    st.caption("시트 구성: 상세데이터 / 제품별요약 / 원육별요약 / 일별요약 / 보고서요약")


def render_loss_tab():
    """로스 관리 탭"""

    menu_options = ["📋 로스 현황"]
    if can_edit("products"):
        menu_options.append("📌 로스 등록")
    menu_options.extend(["📊 로스 분석", "📥 보고서 출력"])

    menu = st.radio("선택", menu_options, horizontal=True, key="loss_menu")

    st.divider()

    if menu == "📋 로스 현황":
        _show_loss_list()
    elif menu == "📌 로스 등록":
        _show_loss_form()
    elif menu == "📊 로스 분석":
        _show_loss_analysis()
    elif menu == "📥 보고서 출력":
        _show_report_download()



# ========================
# 로스 현황
# ========================


# ========================
# 로스 현황 - 데이터 전처리
# ========================

@st.cache_data(ttl=180)
def _prepare_loss_df():
    """로스 데이터를 로드하고 전처리하여 반환 (공통 로직, 캐시 3분)"""
    df = load_losses()

    if df.empty:
        return df

    # 원육 정보: losses DB의 raw_meat 우선, 없으면 products 테이블에서 조인
    if "raw_meat" not in df.columns:
        df["raw_meat"] = ""
    df["raw_meat"] = df["raw_meat"].fillna("").astype(str).str.strip()

    products_df = load_products()
    if not products_df.empty and "product_name" in df.columns:
        product_meat_map = dict(zip(
            products_df["product_name"].astype(str).str.strip(),
            products_df["used_raw_meat"].fillna("").astype(str).str.strip()
        ))
        empty_mask = df["raw_meat"] == ""
        df.loc[empty_mask, "raw_meat"] = df.loc[empty_mask, "product_name"].map(product_meat_map).fillna("")

    if "brand" not in df.columns:
        df["brand"] = ""
    if "tracking_number" not in df.columns:
        df["tracking_number"] = ""
    if "loss_rate" not in df.columns:
        df["loss_rate"] = None
    if "input_kg" not in df.columns:
        df["input_kg"] = 0.0
    if "output_kg" not in df.columns:
        df["output_kg"] = 0.0

    def extract_brand(row):
        if row.get("brand") and str(row["brand"]).strip():
            return str(row["brand"]).strip()
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "브랜드:" in memo_str:
            try:
                part = memo_str.split("브랜드:")[1]
                return part.split("|")[0].strip()
            except:
                pass
        return ""
    df["brand"] = df.apply(extract_brand, axis=1)

    def extract_loss_rate(row):
        in_kg = float(row.get("input_kg", 0) or 0)
        out_kg = float(row.get("output_kg", 0) or 0)
        if in_kg > 0 and out_kg > 0:
            return round((in_kg - out_kg) / in_kg * 100, 2)
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "투입:" in memo_str and "생산:" in memo_str:
            try:
                m_in = float(memo_str.split("투입:")[1].split("kg")[0].strip())
                m_out = float(memo_str.split("생산:")[1].split("kg")[0].strip())
                if m_in > 0 and m_out > 0:
                    return round((m_in - m_out) / m_in * 100, 2)
            except:
                pass
        if pd.notna(row.get("loss_rate")) and row.get("loss_rate") not in [None, 0, 0.0, ""]:
            rate = float(row["loss_rate"])
            if 0 < rate < 1:
                rate = round(rate * 100, 2)
            return rate
        return None
    df["loss_rate"] = df.apply(extract_loss_rate, axis=1)

    def clean_memo(memo):
        memo_str = str(memo).strip() if memo else ""
        if "이력번호:" in memo_str and "브랜드:" in memo_str:
            return ""
        return memo_str
    df["memo_clean"] = df["memo"].apply(clean_memo)

    if "loss_date" in df.columns:
        df["loss_date_dt"] = pd.to_datetime(df["loss_date"], errors="coerce")
        df["month"] = df["loss_date_dt"].dt.to_period("M").astype(str)

    return df


# ========================
# 로스 현황 - 개별 수정 폼
# ========================

def _render_loss_edit_form(row, rid):
    """개별 로스 항목의 수정/삭제 폼 렌더링"""
    products_df_edit = load_products()
    brands_edit = load_brands_list()

    try:
        from views.products.rawmeat_tab import load_raw_meats
        raw_meats_df_edit = load_raw_meats()
        raw_meat_edit_options = []
        if not raw_meats_df_edit.empty:
            for _, rm in raw_meats_df_edit.iterrows():
                name = str(rm.get("name", "")).strip()
                origin = str(rm.get("origin", "")).strip()
                if name:
                    label = f"{name} ({origin})" if origin else name
                    if label not in raw_meat_edit_options:
                        raw_meat_edit_options.append(label)
            raw_meat_edit_options = sorted(raw_meat_edit_options)
    except:
        raw_meat_edit_options = []

    current_date = date.today()
    try:
        current_date = pd.to_datetime(row.get("loss_date")).date()
    except:
        pass
    edit_date = st.date_input("날짜", value=current_date, key=f"edit_date_{rid}")

    current_product_name = str(row.get("product_name", "")).strip()
    if not products_df_edit.empty:
        product_edit_options = products_df_edit.apply(
            lambda r: f"{r['product_code']} | {r['product_name']}", axis=1
        ).tolist()
        default_idx = None
        for i, opt in enumerate(product_edit_options):
            if current_product_name in opt:
                default_idx = i
                break
        edit_product = st.selectbox("제품명", options=product_edit_options, index=default_idx, key=f"edit_product_{rid}")
    else:
        edit_product = st.text_input("제품명", value=current_product_name, key=f"edit_product_{rid}")

    # 제품 변경 시 원육 자동 변경
    prev_edit_product = st.session_state.get(f"_edit_prev_product_{rid}", None)
    if edit_product != prev_edit_product:
        st.session_state[f"_edit_prev_product_{rid}"] = edit_product
        if edit_product and isinstance(edit_product, str) and " | " in edit_product:
            ep_name = edit_product.split(" | ", 1)[1].strip()
            ep_raw_meat = get_raw_meat_by_name(ep_name)
            if ep_raw_meat:
                # 정확히 일치하는 옵션 찾기
                matched = ""
                for opt in raw_meat_edit_options:
                    if opt == ep_raw_meat:
                        matched = opt
                        break
                if not matched:
                    for opt in raw_meat_edit_options:
                        if opt.startswith(ep_raw_meat + " (") or opt == ep_raw_meat:
                            matched = opt
                            break
                st.session_state[f"edit_rawmeat_{rid}"] = matched

    current_raw_meat = str(row.get("raw_meat", "")).strip()
    raw_meat_all_options = [""] + raw_meat_edit_options
    # 초기 매칭 (세션에 값이 없을 때만)
    if f"edit_rawmeat_{rid}" not in st.session_state:
        raw_meat_default_idx = 0
        for i, opt in enumerate(raw_meat_all_options):
            if opt == current_raw_meat:
                raw_meat_default_idx = i
                break
            elif current_raw_meat and opt.startswith(current_raw_meat + " ("):
                raw_meat_default_idx = i
                break
            elif current_raw_meat and opt.startswith(current_raw_meat):
                raw_meat_default_idx = i
                break
        edit_raw_meat_sel = st.selectbox("사용원육", options=raw_meat_all_options, index=raw_meat_default_idx, key=f"edit_rawmeat_{rid}")
    else:
        edit_raw_meat_sel = st.selectbox("사용원육", options=raw_meat_all_options, key=f"edit_rawmeat_{rid}")
    # 원육명+원산지 그대로 저장
    edit_raw_meat = edit_raw_meat_sel if edit_raw_meat_sel else ""

    col_e1, col_e2 = st.columns(2)
    with col_e1:
        if brands_edit:
            brand_all = [""] + brands_edit
            current_brand = str(row.get("brand", "")).strip()
            brand_default_idx = brand_all.index(current_brand) if current_brand in brand_all else 0
            edit_brand = st.selectbox("브랜드", options=brand_all, index=brand_default_idx, key=f"edit_brand_{rid}")
        else:
            edit_brand = st.text_input("브랜드", value=str(row.get("brand", "")).strip(), key=f"edit_brand_{rid}")
    with col_e2:
        edit_tracking = st.text_input("이력번호", value=str(row.get("tracking_number", "")).strip(), key=f"edit_tracking_{rid}")

    col_e3, col_e4 = st.columns(2)
    with col_e3:
        edit_input_kg = st.number_input("투입 kg", min_value=0.0, value=float(row.get("input_kg", 0) or 0),
                                        step=0.1, format="%.1f", key=f"edit_input_{rid}")
    with col_e4:
        edit_output_kg = st.number_input("생산 kg", min_value=0.0, value=float(row.get("output_kg", 0) or 0),
                                         step=0.1, format="%.1f", key=f"edit_output_{rid}")

    if edit_input_kg > 0 and edit_output_kg > 0:
        preview_rate = round((edit_input_kg - edit_output_kg) / edit_input_kg * 100, 2)
        preview_weight = round(edit_input_kg - edit_output_kg, 2)
        if preview_rate >= 0:
            st.info(f"📊 로스율: **{preview_rate}%** | 로스: **{preview_weight}kg**")
        else:
            st.warning(f"⚠️ 생산kg이 투입kg보다 큽니다 (로스율: {preview_rate}%)")

    edit_memo = st.text_input("메모", value=str(row.get("memo_clean", "")).strip(), key=f"edit_memo_{rid}")

    col_btn1, col_btn2 = st.columns([3, 1])
    with col_btn1:
        if st.button("💾 수정 저장", type="primary", key=f"edit_save_{rid}", use_container_width=True):
            try:
                if not products_df_edit.empty and isinstance(edit_product, str) and " | " in edit_product:
                    p_code = edit_product.split(" | ")[0].strip()
                    p_name = edit_product.split(" | ", 1)[1].strip()
                else:
                    p_code = str(row.get("product_code", "")).strip()
                    p_name = edit_product if isinstance(edit_product, str) else current_product_name

                new_loss_rate = None
                new_weight_kg = 0.0
                if edit_input_kg > 0 and edit_output_kg > 0:
                    new_loss_rate = round((edit_input_kg - edit_output_kg) / edit_input_kg * 100, 2)
                    new_weight_kg = round(edit_input_kg - edit_output_kg, 2)

                update_data = {
                    "loss_date": str(edit_date),
                    "product_code": p_code,
                    "product_name": p_name,
                    "raw_meat": edit_raw_meat,
                    "brand": edit_brand.strip() if edit_brand else "",
                    "tracking_number": edit_tracking.strip() if edit_tracking else "",
                    "input_kg": float(edit_input_kg),
                    "output_kg": float(edit_output_kg),
                    "weight_kg": float(new_weight_kg),
                    "memo": edit_memo.strip() if edit_memo else "",
                }
                if new_loss_rate is not None:
                    update_data["loss_rate"] = new_loss_rate

                update_loss(rid, update_data)
                _clear_loss_caches()
                rate_str = f" (로스율: {new_loss_rate}%)" if new_loss_rate is not None else ""
                st.session_state["_loss_edit_success"] = f"✅ '{p_name}' 수정 완료!{rate_str}"
                st.rerun()
            except Exception as e:
                st.error(f"❌ 수정 실패: {str(e)}")
    with col_btn2:
        if st.button("🗑️ 삭제", key=f"edit_del_{rid}"):
            try:
                delete_loss(int(rid))
                _clear_loss_caches()
                st.session_state["_loss_delete_success"] = "✅ 삭제 완료"
                st.rerun()
            except Exception as e:
                st.error(f"❌ 삭제 실패: {str(e)}")


# ========================
# 로스 현황
# ========================

def _show_loss_list():
    st.subheader("📋 로스 현황")

    df = _prepare_loss_df()

    if df.empty:
        st.info("등록된 로스 데이터가 없습니다.")
        return

    # ── 성공/삭제/수정 메시지 표시
    for msg_key in ["_loss_delete_success", "_loss_edit_success"]:
        if st.session_state.get(msg_key):
            st.success(st.session_state[msg_key])
            del st.session_state[msg_key]

    # ── 미입력 건 (최상단)
    incomplete = df[
        (df["output_kg"].fillna(0).astype(float) == 0) | (df["output_kg"].isna()) |
        (df["input_kg"].fillna(0).astype(float) == 0) | (df["input_kg"].isna()) |
        (df["brand"].fillna("").astype(str).str.strip() == "") |
        (df["tracking_number"].fillna("").astype(str).str.strip() == "")
    ]
    if not incomplete.empty and can_edit("products"):
        st.markdown(f"#### ⚠️ 미입력 건 ({len(incomplete)}건)")
        brands = load_brands_list()

        # 원육 목록 로드 (미입력 건에서 원육 수정용)
        try:
            from views.products.rawmeat_tab import load_raw_meats
            raw_meats_df_inc = load_raw_meats()
            raw_meat_inc_options = []
            if not raw_meats_df_inc.empty:
                for _, rm in raw_meats_df_inc.iterrows():
                    name = str(rm.get("name", "")).strip()
                    origin = str(rm.get("origin", "")).strip()
                    if name:
                        label = f"{name} ({origin})" if origin else name
                        if label not in raw_meat_inc_options:
                            raw_meat_inc_options.append(label)
                raw_meat_inc_options = sorted(raw_meat_inc_options)
        except:
            raw_meat_inc_options = []

        # 날짜별 그룹핑 (최신 날짜 먼저)
        inc_dates = sorted(incomplete["loss_date"].unique().tolist(), reverse=True)
        for loss_date_val in inc_dates:
            date_rows = incomplete[incomplete["loss_date"] == loss_date_val]
            st.markdown(f"**📅 {loss_date_val}** ({len(date_rows)}건)")

            for _, row in date_rows.iterrows():
                rid = row["id"]
                cur_brand = str(row.get("brand", "")).strip()
                cur_tracking = str(row.get("tracking_number", "")).strip()
                cur_input = float(row.get("input_kg", 0) or 0)
                cur_output = float(row.get("output_kg", 0) or 0)
                cur_memo_clean = str(row.get("memo_clean", "")).strip()
                cur_raw_meat = str(row.get("raw_meat", "")).strip()

                with st.expander(f"🔸 {row.get('product_name', '')}"):
                    # 사용원육 선택
                    raw_meat_inc_all = [""] + raw_meat_inc_options
                    if f"inc_rawmeat_{rid}" not in st.session_state:
                        raw_meat_inc_default_idx = 0
                        for i, opt in enumerate(raw_meat_inc_all):
                            if opt == cur_raw_meat:
                                raw_meat_inc_default_idx = i
                                break
                            elif cur_raw_meat and opt.startswith(cur_raw_meat + " ("):
                                raw_meat_inc_default_idx = i
                                break
                            elif cur_raw_meat and opt.startswith(cur_raw_meat):
                                raw_meat_inc_default_idx = i
                                break
                        new_raw_meat_sel = st.selectbox("사용원육", options=raw_meat_inc_all, index=raw_meat_inc_default_idx, key=f"inc_rawmeat_{rid}")
                    else:
                        new_raw_meat_sel = st.selectbox("사용원육", options=raw_meat_inc_all, key=f"inc_rawmeat_{rid}")
                    new_raw_meat = new_raw_meat_sel if new_raw_meat_sel else ""

                    col_i1, col_i2 = st.columns(2)
                    with col_i1:
                        if brands:
                            brand_all = [""] + brands
                            brand_idx = brand_all.index(cur_brand) if cur_brand in brand_all else 0
                            new_brand = st.selectbox("브랜드", options=brand_all, index=brand_idx, key=f"inc_brand_{rid}")
                        else:
                            new_brand = st.text_input("브랜드", value=cur_brand, key=f"inc_brand_{rid}")
                    with col_i2:
                        new_tracking = st.text_input("이력번호", value=cur_tracking, key=f"inc_tracking_{rid}")

                    col_i3, col_i4 = st.columns(2)
                    with col_i3:
                        new_input = st.number_input("투입 kg", min_value=0.0, value=cur_input, step=0.1, key=f"inc_input_{rid}")
                    with col_i4:
                        new_output = st.number_input("생산 kg", min_value=0.0, value=cur_output, step=0.1, key=f"inc_output_{rid}")

                    if new_input > 0 and new_output > 0:
                        preview_rate = round((new_input - new_output) / new_input * 100, 2)
                        st.info(f"📊 로스율: **{preview_rate}%** | 로스: **{round(new_input - new_output, 2)}kg**")

                    new_memo = st.text_input("메모", value=cur_memo_clean, key=f"inc_memo_{rid}")

                    col_inc_btn1, col_inc_btn2 = st.columns([3, 1])
                    with col_inc_btn1:
                        if st.button("💾 저장", key=f"inc_save_{rid}", type="primary", use_container_width=True):
                            try:
                                new_loss_rate = round((new_input - new_output) / new_input * 100, 2) if new_input > 0 and new_output > 0 else None
                                new_weight = round(new_input - new_output, 2) if new_input > 0 and new_output > 0 else 0

                                update_data = {
                                    "raw_meat": new_raw_meat,
                                    "brand": new_brand.strip() if new_brand else "",
                                    "tracking_number": new_tracking.strip() if new_tracking else "",
                                    "input_kg": float(new_input),
                                    "output_kg": float(new_output),
                                    "weight_kg": float(new_weight),
                                    "memo": new_memo.strip() if new_memo else "",
                                }
                                if new_loss_rate is not None:
                                    update_data["loss_rate"] = new_loss_rate

                                update_loss(rid, update_data)
                                _clear_loss_caches()
                                rate_str = f" (로스율: {new_loss_rate}%)" if new_loss_rate is not None else ""
                                st.session_state["_loss_edit_success"] = f"✅ 저장 완료!{rate_str}"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 저장 실패: {str(e)}")
                    with col_inc_btn2:
                        if st.button("🗑️ 삭제", key=f"inc_del_{rid}", use_container_width=True):
                            try:
                                delete_loss(int(rid))
                                _clear_loss_caches()
                                st.session_state["_loss_delete_success"] = "✅ 삭제 완료"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 삭제 실패: {str(e)}")
        st.divider()

    # ── 검색 필터 (날짜 / 원육 / 제품)
    if "loss_date" not in df.columns or df["loss_date"].isna().all():
        st.warning("날짜 데이터가 없습니다.")
        return

    df["loss_date_dt"] = pd.to_datetime(df["loss_date"], errors="coerce")
    min_date = df["loss_date_dt"].min().date()
    max_date = df["loss_date_dt"].max().date()

    # 필터 옵션 목록 생성
    products_list = sorted(df["product_name"].fillna("").astype(str).str.strip().unique().tolist())
    products_list = [p for p in products_list if p]
    unique_meats = sorted([m for m in df["raw_meat"].fillna("").astype(str).str.strip().unique().tolist() if m])

    col_d1, col_d2, col_f1, col_f2 = st.columns([1, 1, 1, 1])
    with col_d1:
        start_date = st.date_input(
            "📅 시작일", value=max_date,
            min_value=min_date, max_value=max_date,
            key="loss_start_date"
        )
    with col_d2:
        end_date = st.date_input(
            "📅 종료일", value=max_date,
            min_value=min_date, max_value=max_date,
            key="loss_end_date"
        )
    with col_f1:
        selected_meat_f = st.selectbox("🥩 원육", options=["전체"] + unique_meats, index=0, key="loss_meat_filter")
    with col_f2:
        selected_product_f = st.selectbox("📦 제품", options=["전체"] + products_list, index=0, key="loss_product_filter")

    # 필터 적용
    filtered_df = df.copy()
    filtered_df = filtered_df[
        (filtered_df["loss_date_dt"].dt.date >= start_date) &
        (filtered_df["loss_date_dt"].dt.date <= end_date)
    ]
    if selected_meat_f != "전체":
        filtered_df = filtered_df[filtered_df["raw_meat"].fillna("").astype(str).str.strip() == selected_meat_f]
    if selected_product_f != "전체":
        filtered_df = filtered_df[filtered_df["product_name"].fillna("").astype(str).str.strip() == selected_product_f]

    # ── 요약 메트릭 (선택된 제품 평균로스 포함)
    f_rates = filtered_df["loss_rate"].dropna()
    if selected_product_f != "전체":
        # 선택된 제품의 전체 기간 평균 로스율
        product_all = df[df["product_name"].fillna("").astype(str).str.strip() == selected_product_f]
        product_avg_rates = product_all["loss_rate"].dropna()
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("건수", f"{len(filtered_df)}건")
        with col2:
            st.metric("총 로스", f"{filtered_df['weight_kg'].sum():,.1f}kg")
        with col3:
            st.metric("총 투입", f"{filtered_df['input_kg'].fillna(0).astype(float).sum():,.1f}kg")
        with col4:
            if not f_rates.empty:
                st.metric("평균 로스율", f"{f_rates.mean():.1f}%")
            else:
                st.metric("평균 로스율", "-")
        with col5:
            if not product_avg_rates.empty:
                st.metric(f"📌 {selected_product_f} 평균로스", f"{product_avg_rates.mean():.1f}%")
            else:
                st.metric(f"📌 {selected_product_f} 평균로스", "-")
    else:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("건수", f"{len(filtered_df)}건")
        with col2:
            st.metric("총 로스", f"{filtered_df['weight_kg'].sum():,.1f}kg")
        with col3:
            st.metric("총 투입", f"{filtered_df['input_kg'].fillna(0).astype(float).sum():,.1f}kg")
        with col4:
            if not f_rates.empty:
                st.metric("평균 로스율", f"{f_rates.mean():.1f}%")
            else:
                st.metric("평균 로스율", "-")

    if filtered_df.empty:
        st.info("선택한 조건에 해당하는 로스 데이터가 없습니다.")
        return

    st.divider()

    # ── 상세 테이블
    detail_cols = ["loss_date", "product_name", "raw_meat", "brand", "tracking_number",
                   "input_kg", "output_kg", "weight_kg", "loss_rate", "memo_clean"]
    detail_cols = [c for c in detail_cols if c in filtered_df.columns]
    detail_names = {
        "loss_date": "날짜", "product_name": "제품명", "raw_meat": "원육", "brand": "브랜드",
        "tracking_number": "이력번호", "input_kg": "투입(kg)",
        "output_kg": "생산(kg)", "weight_kg": "로스(kg)",
        "loss_rate": "로스율(%)", "memo_clean": "메모"
    }
    st.dataframe(filtered_df[detail_cols].rename(columns=detail_names),
                 use_container_width=True, hide_index=True)

    # ── 수정 / 삭제
    if can_edit("products"):
        st.divider()
        st.markdown("#### ✏️ 수정 / 🗑️ 삭제")
        for _, row in filtered_df.iterrows():
            rid = row["id"]
            rate_str = f" | 로스율: {row['loss_rate']:.1f}%" if pd.notna(row.get("loss_rate")) else ""
            label_str = f"{row.get('loss_date', '')} | {row.get('product_name', '')} | {row.get('brand', '')} | 로스: {row.get('weight_kg', 0)}kg{rate_str}"
            with st.expander(f"🔸 {label_str}", expanded=False):
                _render_loss_edit_form(row, rid)

    # ── 월별 로스 요약
    st.divider()
    st.markdown("#### 📅 월별 로스 요약")

    if "month" in filtered_df.columns and filtered_df["month"].notna().any():
        months_sorted = sorted(filtered_df["month"].dropna().unique().tolist(), reverse=True)
        monthly_summary = []
        for m in months_sorted:
            m_df = filtered_df[filtered_df["month"] == m]
            m_rates_s = m_df["loss_rate"].dropna()
            monthly_summary.append({
                "월": m,
                "건수": len(m_df),
                "총 로스(kg)": round(m_df["weight_kg"].sum(), 1),
                "총 투입(kg)": round(m_df["input_kg"].fillna(0).astype(float).sum(), 1),
                "총 생산(kg)": round(m_df["output_kg"].fillna(0).astype(float).sum(), 1),
                "평균 로스율(%)": round(m_rates_s.mean(), 1) if not m_rates_s.empty else None,
                "최고 로스율(%)": round(m_rates_s.max(), 1) if not m_rates_s.empty else None,
            })
        monthly_df = pd.DataFrame(monthly_summary)
        st.dataframe(
            monthly_df.style.format({
                "총 로스(kg)": "{:,.1f}", "총 투입(kg)": "{:,.1f}",
                "총 생산(kg)": "{:,.1f}", "평균 로스율(%)": "{:.1f}",
                "최고 로스율(%)": "{:.1f}",
            }, na_rep="-"),
            use_container_width=True, hide_index=True
        )
    else:
        st.info("월별 데이터가 없습니다.")

def _show_loss_form():
    """로스 등록 (제품명/사용원육/브랜드/이력번호/투입kg/생산kg/메모)"""
    st.markdown("#### 📌 로스 등록")

    # 등록 성공 알림
    if st.session_state.get("_loss_reg_success"):
        st.success(st.session_state["_loss_reg_success"])
        st.toast(st.session_state["_loss_reg_success"])
        del st.session_state["_loss_reg_success"]

    # 폼 리셋용 카운터 (등록 성공 시 증가 → 위젯 key가 바뀌어 초기화됨)
    if "_loss_form_counter" not in st.session_state:
        st.session_state["_loss_form_counter"] = 0
    fc = st.session_state["_loss_form_counter"]

    products_df = load_products()
    brands = load_brands_list()

    # 원육 목록 로드 (원산지 포함)
    from views.products.rawmeat_tab import load_raw_meats
    raw_meats_df = load_raw_meats()

    # 원육(원산지) 선택 옵션 생성 (중복 없이)
    raw_meat_options = []
    if not raw_meats_df.empty:
        for _, rm in raw_meats_df.iterrows():
            name = str(rm.get("name", "")).strip()
            origin = str(rm.get("origin", "")).strip()
            if name:
                label = f"{name} ({origin})" if origin else name
                if label not in raw_meat_options:
                    raw_meat_options.append(label)
        raw_meat_options = sorted(raw_meat_options)

    # 제품 선택
    if not products_df.empty:
        product_options = products_df.apply(
            lambda r: f"{r['product_code']} | {r['product_name']}", axis=1
        ).tolist()
        selected_product = st.selectbox(
            "제품명", options=product_options, index=None,
            placeholder="제품을 선택하세요...", key=f"loss_reg_product_{fc}"
        )
    else:
        selected_product = None
        st.warning("등록된 제품이 없습니다. 제품 탭에서 먼저 등록해주세요.")

    # 제품 변경 감지 → 원육 자동 변경
    prev_product = st.session_state.get(f"_loss_reg_prev_product_{fc}", None)
    if selected_product != prev_product:
        st.session_state[f"_loss_reg_prev_product_{fc}"] = selected_product
        if selected_product:
            p_name = selected_product.split(" | ", 1)[1] if " | " in selected_product else ""
            # 제품의 used_raw_meat 값 가져오기 (이미 "원육명 (원산지)" 형태)
            default_raw_meat = get_raw_meat_by_name(p_name)
            # 옵션 목록에서 매칭
            matched_option = ""
            if default_raw_meat:
                # 정확히 일치하는 옵션 찾기
                for opt in raw_meat_options:
                    if opt == default_raw_meat:
                        matched_option = opt
                        break
                # 하위호환: 원육명만 저장된 경우 (원산지 없이)
                if not matched_option:
                    for opt in raw_meat_options:
                        if opt.startswith(default_raw_meat + " (") or opt == default_raw_meat:
                            matched_option = opt
                            break
            st.session_state[f"loss_reg_rawmeat_{fc}"] = matched_option
        else:
            st.session_state[f"loss_reg_rawmeat_{fc}"] = ""

    # 사용원육: 수정 가능한 selectbox
    raw_meat_selection = st.selectbox(
        "사용원육 (원산지)", options=[""] + raw_meat_options,
        key=f"loss_reg_rawmeat_{fc}"
    )
    # 원육명+원산지 그대로 저장
    raw_meat = raw_meat_selection if raw_meat_selection else ""

    col1, col2 = st.columns(2)
    with col1:
        brand = st.selectbox("브랜드", options=[""] + brands, index=0,
                             placeholder="브랜드 선택...", key=f"loss_reg_brand_{fc}")
    with col2:
        tracking_number = st.text_input("이력번호", placeholder="이력번호 입력", key=f"loss_reg_tracking_{fc}")

    col3, col4 = st.columns(2)
    with col3:
        input_kg = st.number_input("투입 kg", min_value=0.0, value=0.0, step=0.1, key=f"loss_reg_input_kg_{fc}")
    with col4:
        output_kg = st.number_input("생산 kg", min_value=0.0, value=0.0, step=0.1, key=f"loss_reg_output_kg_{fc}")

    # 로스율 미리보기
    if input_kg > 0 and output_kg > 0:
        loss_rate = round((input_kg - output_kg) / input_kg * 100, 2)
        weight_kg = round(input_kg - output_kg, 2)
        if loss_rate >= 0:
            st.info(f"📊 로스율: **{loss_rate}%** | 로스 중량: **{weight_kg}kg**")
        else:
            st.warning(f"⚠️ 생산kg이 투입kg보다 큽니다 (로스율: {loss_rate}%)")
    elif input_kg > 0 and output_kg == 0:
        st.caption("💡 생산kg은 나중에 로스 현황에서 수정할 수 있습니다.")

    memo = st.text_input("메모", placeholder="메모 (선택)", key=f"loss_reg_memo_{fc}")

    loss_date = st.date_input("날짜", value=date.today(), key=f"loss_reg_date_{fc}")

    if st.button("💾 로스 등록", type="primary", use_container_width=True):
        if not selected_product:
            st.error("제품을 선택해주세요.")
        else:
            p_code = selected_product.split(" | ")[0].strip()
            p_name = selected_product.split(" | ", 1)[1].strip() if " | " in selected_product else ""
            loss_rate = round((input_kg - output_kg) / input_kg * 100, 2) if input_kg > 0 and output_kg > 0 else None
            weight_kg = round(input_kg - output_kg, 2) if output_kg > 0 else 0

            try:
                insert_loss(
                    loss_date=loss_date,
                    product_code=p_code,
                    product_name=p_name,
                    weight_kg=weight_kg,
                    memo=memo.strip() if memo else "",
                    brand=brand,
                    tracking_number=tracking_number,
                    input_kg=input_kg,
                    output_kg=output_kg,
                    loss_rate=loss_rate,
                    raw_meat=raw_meat,
                )
                _clear_loss_caches()
                if loss_rate is not None:
                    st.session_state["_loss_reg_success"] = f"✅ '{p_name}' 로스 등록 완료! (로스율: {loss_rate}%)"
                else:
                    st.session_state["_loss_reg_success"] = f"✅ '{p_name}' 로스 등록 완료! (생산kg 미입력)"
                # 카운터 증가 → 다음 rerun에서 모든 위젯 key가 바뀌어 초기화됨
                st.session_state["_loss_form_counter"] = fc + 1
                st.rerun()
            except Exception as e:
                st.error(f"❌ 등록 실패: {str(e)}")


# ========================
# 로스 분석
# ========================

def _show_loss_analysis():
    st.subheader("📊 로스 분석")

    df = _prepare_loss_df()

    if df.empty:
        st.info("등록된 로스 데이터가 없습니다.")
        return

    # _prepare_loss_df에서 계산된 loss_rate를 loss_rate_calc로 별칭
    df = df.copy()
    df["loss_rate_calc"] = df["loss_rate"]

    # ========================
    # 1. 핵심 요약 지표
    # ========================
    st.markdown("### 핵심 지표")
    rates = df["loss_rate_calc"].dropna()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("총 건수", f"{len(df)}건")
    with col2:
        st.metric("총 로스 중량", f"{df['weight_kg'].sum():,.1f}kg")
    with col3:
        if not rates.empty:
            st.metric("평균 로스율", f"{rates.mean():.1f}%")
        else:
            st.metric("평균 로스율", "-")
    with col4:
        if not rates.empty:
            st.metric("최고 로스율", f"{rates.max():.1f}%")
        else:
            st.metric("최고 로스율", "-")

    st.divider()

    # ========================
    # 2. 제품별 로스율 순위 (TOP 10)
    # ========================
    st.markdown("### 🏆 제품별 로스율 순위")

    has_rate = df[df["loss_rate_calc"].notna()].copy()

    if not has_rate.empty:
        product_rate = has_rate.groupby("product_name").agg(
            평균로스율=("loss_rate_calc", "mean"),
            건수=("id", "count"),
            총로스중량=("weight_kg", "sum")
        ).sort_values("평균로스율", ascending=False).reset_index()
        product_rate["평균로스율"] = product_rate["평균로스율"].round(1)
        product_rate["총로스중량"] = product_rate["총로스중량"].round(1)

        top_n = min(10, len(product_rate))

        # 차트
        chart_df = product_rate.head(top_n).copy()
        chart_df = chart_df.rename(columns={"product_name": "제품명"})
        st.bar_chart(chart_df.set_index("제품명")["평균로스율"], use_container_width=True)

        # 순위 테이블
        display_rate = product_rate.head(top_n).copy()
        display_rate.insert(0, "순위", range(1, top_n + 1))
        display_rate = display_rate.rename(columns={
            "product_name": "제품명", "평균로스율": "평균 로스율(%)",
            "건수": "생산 건수", "총로스중량": "총 로스(kg)"
        })
        st.dataframe(display_rate, use_container_width=True, hide_index=True)
    else:
        st.info("로스율 데이터가 없습니다.")

    st.divider()

    # ========================
    # 3. 원육별 로스 분석
    # ========================
    st.markdown("### 🥩 원육별 로스 분석")

    meat_df = df[df["raw_meat"] != ""].copy()
    if not meat_df.empty:
        meat_rate = meat_df.groupby("raw_meat").agg(
            건수=("id", "count"),
            총로스중량=("weight_kg", "sum")
        ).sort_values("총로스중량", ascending=False).reset_index()
        meat_rate["총로스중량"] = meat_rate["총로스중량"].round(1)

        # 로스율이 있는 경우 평균 로스율도 추가
        meat_has_rate = meat_df[meat_df["loss_rate_calc"].notna()]
        if not meat_has_rate.empty:
            meat_avg_rate = meat_has_rate.groupby("raw_meat")["loss_rate_calc"].mean().round(1)
            meat_rate["평균로스율"] = meat_rate["raw_meat"].map(meat_avg_rate).fillna("-")

        meat_rate = meat_rate.rename(columns={
            "raw_meat": "원육", "건수": "생산 건수",
            "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
        })

        col1, col2 = st.columns(2)
        with col1:
            # 원육별 로스 중량 차트
            st.markdown("**로스 중량 (kg)**")
            chart_meat = meat_rate.set_index("원육")
            st.bar_chart(chart_meat["총 로스(kg)"], use_container_width=True)
        with col2:
            # 테이블
            st.markdown("**상세 데이터**")
            st.dataframe(meat_rate, use_container_width=True, hide_index=True)
    else:
        st.info("원육 정보가 등록된 로스 데이터가 없습니다.")

    st.divider()

    # ========================
    # 4. 월별 로스 추이
    # ========================
    st.markdown("### 📈 월별 로스 추이")

    if "loss_date_dt" in df.columns and df["loss_date_dt"].notna().any():
        df["month"] = df["loss_date_dt"].dt.to_period("M").astype(str)
        monthly = df.groupby("month").agg(
            건수=("id", "count"),
            총로스중량=("weight_kg", "sum")
        ).reset_index()
        monthly["총로스중량"] = monthly["총로스중량"].round(1)

        # 로스율 월별 평균
        monthly_has_rate = df[df["loss_rate_calc"].notna()]
        if not monthly_has_rate.empty:
            monthly_avg = monthly_has_rate.groupby("month")["loss_rate_calc"].mean().round(1)
            monthly["평균로스율"] = monthly["month"].map(monthly_avg).fillna(0)

        monthly = monthly.rename(columns={
            "month": "월", "건수": "생산 건수",
            "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
        })

        # 차트
        chart_monthly = monthly.set_index("월")
        tab1, tab2 = st.tabs(["로스 중량 추이", "로스율 추이"])
        with tab1:
            st.line_chart(chart_monthly["총 로스(kg)"], use_container_width=True)
        with tab2:
            if "평균 로스율(%)" in chart_monthly.columns:
                st.line_chart(chart_monthly["평균 로스율(%)"], use_container_width=True)
            else:
                st.info("로스율 데이터가 없습니다.")

        # 테이블
        st.dataframe(monthly, use_container_width=True, hide_index=True)
    else:
        st.info("날짜 데이터가 없습니다.")

    st.divider()

    # ========================
    # 5. 주간별 로스 추이
    # ========================
    st.markdown("### 📅 주간별 로스 추이")

    if "loss_date_dt" in df.columns and df["loss_date_dt"].notna().any():
        df["week"] = df["loss_date_dt"].dt.isocalendar().week.astype(str)
        df["year_week"] = df["loss_date_dt"].dt.strftime("%Y-W") + df["week"]

        weekly = df.groupby("year_week").agg(
            건수=("id", "count"),
            총로스중량=("weight_kg", "sum")
        ).sort_index().reset_index()
        weekly["총로스중량"] = weekly["총로스중량"].round(1)

        weekly_has_rate = df[df["loss_rate_calc"].notna()]
        if not weekly_has_rate.empty:
            weekly_avg = weekly_has_rate.groupby("year_week")["loss_rate_calc"].mean().round(1)
            weekly["평균로스율"] = weekly["year_week"].map(weekly_avg).fillna(0)

        weekly = weekly.rename(columns={
            "year_week": "주차", "건수": "생산 건수",
            "총로스중량": "총 로스(kg)", "평균로스율": "평균 로스율(%)"
        })

        chart_weekly = weekly.set_index("주차")
        st.line_chart(chart_weekly[["총 로스(kg)"]], use_container_width=True)
        st.dataframe(weekly, use_container_width=True, hide_index=True)

    st.divider()

    # ========================
    # 6. 로스율 높은 건 (경고 목록)
    # ========================
    st.markdown("### ⚠️ 고로스율 경고 (5% 이상)")

    high_loss = df[df["loss_rate_calc"].notna() & (df["loss_rate_calc"] >= 5)].copy()
    if not high_loss.empty:
        high_loss = high_loss.sort_values("loss_rate_calc", ascending=False)
        display_high = high_loss[["loss_date", "product_name", "loss_rate_calc", "weight_kg", "raw_meat"]].copy()
        display_high["loss_rate_calc"] = display_high["loss_rate_calc"].round(1)
        display_high["weight_kg"] = display_high["weight_kg"].round(1)
        display_high = display_high.rename(columns={
            "loss_date": "날짜", "product_name": "제품명",
            "loss_rate_calc": "로스율(%)", "weight_kg": "로스(kg)", "raw_meat": "원육"
        })
        st.dataframe(display_high, use_container_width=True, hide_index=True)
        st.caption(f"총 {len(high_loss)}건 | 5% 이상 로스율 발생 건")
    else:
        st.success("5% 이상 로스율 발생 건이 없습니다.")
