import streamlit as st
import pandas as pd
from io import BytesIO
from views.products import supabase, load_products
from datetime import date, datetime


# ========================
# 로스 DB 함수
# ========================

@st.cache_data(ttl=60)
def load_losses():
    """losses 테이블에서 로스 데이터 로드"""
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


def delete_loss(loss_id):
    supabase.table("losses").delete().eq("id", loss_id).execute()


def update_loss(loss_id, data: dict):
    """losses 테이블의 특정 행 업데이트"""
    supabase.table("losses").update(data).eq("id", loss_id).execute()


# ========================
# 생산기록 DB 함수
# ========================

def load_production_records(week_start=None):
    """production_records 테이블에서 생산기록 로드"""
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


def insert_production_record(data):
    """생산기록 신규 등록"""
    supabase.table("production_records").insert(data).execute()


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

    return loss_kg, loss_rate, today


def delete_production_record(record_id):
    supabase.table("production_records").delete().eq("id", record_id).execute()


# ========================
# 스케줄 데이터 조회
# ========================

def get_schedule_weeks():
    """schedules 테이블에서 주차 목록 조회"""
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


def load_schedule_products(week_start):
    """해당 주차의 스케줄 제품 목록 로드"""
    try:
        result = supabase.table("schedules").select("*").eq(
            "week_start", str(week_start)
        ).order("id").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame()


def load_brands_list():
    """brands 테이블에서 브랜드명 목록 로드"""
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


def _show_excel_upload():
    st.subheader("📤 엑셀 파일로 로스 데이터 업로드")

    # 성공 메시지 표시
    if st.session_state.get("excel_upload_success"):
        st.success(st.session_state["excel_upload_success"])
        del st.session_state["excel_upload_success"]

    st.info("""
**엑셀 파일 형식 안내**

아래 컬럼명이 포함된 엑셀 파일을 업로드해주세요:

| 컬럼명 | 설명 | 필수 |
|--------|------|------|
| loss_date (또는 날짜, 생산 일자) | 로스 날짜 (예: 2025-01-15) | ✅ |
| product_name (또는 제품명) | 제품명 | ✅ |
| 사용원육 (또는 원육) | 사용 원육 | |
| 브랜드 | 브랜드명 | |
| 이력번호 | 이력번호 | |
| 투입(kg) | 투입 중량 | |
| 생산(kg) | 생산 중량 | |
| 로스(kg) (또는 중량) | 로스 중량(kg) | |
| 로스율(%) | 로스율 | |
| memo (또는 메모, 비고) | 메모 | |
    """)

    uploaded_file = st.file_uploader(
        "엑셀 파일 선택 (.xlsx, .xls, .csv)",
        type=["xlsx", "xls", "csv"],
        key="loss_excel_uploader"
    )

    if uploaded_file is not None:
        try:
            # 파일 읽기
            if uploaded_file.name.endswith(".csv"):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)

            # 한글 컬럼명 매핑
            col_map = {
                "날짜": "loss_date",
                "생산 일자": "loss_date",
                "제품명": "product_name",
                "제품코드": "product_code",
                "사용원육": "raw_meat",
                "사용 원육": "raw_meat",
                "원육": "raw_meat",
                "원육종류": "raw_meat",
                "브랜드": "brand",
                "이력번호": "tracking_number",
                "투입(kg)": "input_kg",
                "투입(Kg)": "input_kg",
                "투입kg": "input_kg",
                "투입Kg": "input_kg",
                "생산(kg)": "output_kg",
                "생산(Kg)": "output_kg",
                "생산kg": "output_kg",
                "생산Kg": "output_kg",
                "로스(kg)": "weight_kg",
                "로스(Kg)": "weight_kg",
                "로스kg": "weight_kg",
                "로스Kg": "weight_kg",
                "로스율(%)": "loss_rate",
                "로스율": "loss_rate",
                "중량": "weight_kg",
                "중량(kg)": "weight_kg",
                "중량(Kg)": "weight_kg",
                "메모": "memo",
                "비고": "memo"
            }
            df.rename(columns=col_map, inplace=True)

            st.write(f"**읽어온 데이터: {len(df)}행**")
            st.dataframe(df.head(20), use_container_width=True, hide_index=True)

            if len(df) > 20:
                st.caption(f"... 외 {len(df) - 20}행 더 있음")

            # 필수 컬럼 확인
            missing = []
            if "loss_date" not in df.columns:
                missing.append("loss_date (또는 날짜)")
            if "product_name" not in df.columns:
                missing.append("product_name (또는 제품명)")

            if missing:
                st.error(f"필수 컬럼이 누락되었습니다: {', '.join(missing)}")
                return

            # 업로드 버튼
            st.divider()
            col1, col2 = st.columns([1, 3])
            with col1:
                upload_btn = st.button("🚀 업로드 실행", type="primary", use_container_width=True)
            with col2:
                st.caption(f"총 {len(df)}건의 데이터가 losses 테이블에 추가됩니다.")

            if upload_btn:
                success_count = 0
                error_count = 0
                errors = []

                progress = st.progress(0, text="업로드 중...")

                for idx, row in df.iterrows():
                    try:
                        loss_date = str(row.get("loss_date", "")).strip()
                        product_name = str(row.get("product_name", "")).strip()
                        product_code = str(row.get("product_code", "")).strip() if pd.notna(row.get("product_code")) else ""
                        memo = str(row.get("memo", "")).strip() if pd.notna(row.get("memo")) else ""

                        # weight_kg: weight_kg 컬럼 우선, 없으면 loss_kg 사용
                        weight_kg = 0.0
                        if pd.notna(row.get("weight_kg")):
                            weight_kg = float(row["weight_kg"])
                        elif pd.notna(row.get("loss_kg")):
                            weight_kg = float(row["loss_kg"])

                        if not loss_date or not product_name:
                            error_count += 1
                            errors.append(f"행 {idx + 1}: 날짜 또는 제품명 누락")
                            continue

                        # 날짜 형식 변환
                        try:
                            parsed_date = pd.to_datetime(loss_date)
                            loss_date = parsed_date.strftime("%Y-%m-%d")
                        except:
                            pass

                        # 추가 컬럼 처리
                        brand = str(row.get("brand", "")).strip() if pd.notna(row.get("brand")) else ""
                        tracking_number = str(row.get("tracking_number", "")).strip() if pd.notna(row.get("tracking_number")) else ""
                        raw_meat = str(row.get("raw_meat", "")).strip() if pd.notna(row.get("raw_meat")) else ""
                        # raw_meat가 비어있으면 products 테이블에서 보충
                        if not raw_meat and product_name:
                            raw_meat = get_raw_meat_by_name(product_name)
                        input_kg = float(row["input_kg"]) if pd.notna(row.get("input_kg")) else 0.0
                        output_kg = float(row["output_kg"]) if pd.notna(row.get("output_kg")) else 0.0

                        # loss_rate: 엑셀에 있으면 사용, 없으면 input/output에서 계산
                        loss_rate = None
                        if pd.notna(row.get("loss_rate")):
                            loss_rate = float(row["loss_rate"])
                            # 엑셀에서 퍼센트 서식이 소수로 읽힌 경우 (0.0369 → 3.69%)
                            if 0 < loss_rate < 1:
                                loss_rate = round(loss_rate * 100, 2)
                        elif input_kg > 0 and output_kg > 0:
                            loss_rate = round((input_kg - output_kg) / input_kg * 100, 2)

                        # weight_kg가 아직 0이면 input-output으로 계산
                        if weight_kg == 0.0 and input_kg > 0 and output_kg > 0:
                            weight_kg = round(input_kg - output_kg, 2)

                        insert_data = {
                            "loss_date": loss_date,
                            "product_code": product_code,
                            "product_name": product_name,
                            "weight_kg": weight_kg,
                            "memo": memo,
                            "brand": brand,
                            "tracking_number": tracking_number,
                            "input_kg": input_kg,
                            "output_kg": output_kg,
                            "raw_meat": raw_meat,
                        }
                        if loss_rate is not None:
                            insert_data["loss_rate"] = loss_rate

                        supabase.table("losses").insert(insert_data).execute()

                        success_count += 1
                    except Exception as e:
                        error_count += 1
                        errors.append(f"행 {idx + 1}: {str(e)[:50]}")

                    progress.progress((idx + 1) / len(df), text=f"업로드 중... ({idx + 1}/{len(df)})")

                progress.empty()

                # 결과 표시
                if success_count > 0:
                    st.session_state["excel_upload_success"] = f"✅ {success_count}건 업로드 완료!"
                    st.toast(f"✅ {success_count}건 업로드 완료!")
                if error_count > 0:
                    st.warning(f"⚠️ {error_count}건 실패")
                    with st.expander("오류 상세"):
                        for err in errors:
                            st.text(err)

                if success_count > 0:
                    load_losses.clear()
                    st.rerun()

        except Exception as e:
            st.error(f"파일 읽기 오류: {str(e)}")


# ========================
# 렌더링
# ========================

def render_loss_tab():
    """로스 관리 탭"""

    menu = st.radio("선택", [
        "📝 생산 기록",
        "✏️ 생산 등록",
        "📋 로스 현황",
        "📌 로스 등록",
        "📊 로스 분석",
        "📤 엑셀 업로드",
        "📥 보고서 출력"
    ], horizontal=True, key="loss_menu")

    st.divider()

    if menu == "📝 생산 기록":
        _show_production_record()
    elif menu == "✏️ 생산 등록":
        _show_production_form()
    elif menu == "📋 로스 현황":
        _show_loss_list()
    elif menu == "📌 로스 등록":
        _show_loss_form()
    elif menu == "📊 로스 분석":
        _show_loss_analysis()
    elif menu == "📥 보고서 출력":
        _show_report_download()
    elif menu == "📤 엑셀 업로드":
        _show_excel_upload()


# ========================
# 생산기록
# ========================

def _show_production_record():
    st.subheader("📝 생산 기록")

    # 성공 메시지 표시
    if 'prod_record_msg' in st.session_state:
        st.success(st.session_state['prod_record_msg'])
        try:
            st.toast(st.session_state['prod_record_msg'], icon="✅")
        except:
            pass
        del st.session_state['prod_record_msg']

    # 1) 주차 선택
    weeks = get_schedule_weeks()
    if not weeks:
        st.info("저장된 스케줄이 없습니다. 먼저 스케줄을 생성해주세요.")
        return

    week_options = [f"{w[0]} ~ {w[1]}" for w in weeks]
    selected_week = st.selectbox("📅 주차 선택", week_options, key="prod_rec_week")

    week_start = weeks[week_options.index(selected_week)][0]

    # DB에 해당 주차 생산기록이 이미 있는지 확인
    records_df = load_production_records(week_start)
    has_existing = not records_df.empty

    if has_existing:
        # 이미 불러온 기록이 있으면 바로 표시
        st.caption(f"✅ {week_start} 주차 생산기록이 로드되었습니다.")
    else:
        # 불러온 적 없는 주차 → 불러오기 버튼
        st.caption("주차를 선택한 후 '불러오기' 버튼을 눌러주세요.")
        if st.button("📥 불러오기", key="prod_rec_load", type="primary"):
            schedule_df = load_schedule_products(week_start)
            if schedule_df.empty:
                st.warning("해당 주차에 스케줄이 없습니다.")
                return
            try:
                new_records = []
                for _, row in schedule_df.iterrows():
                    new_records.append({
                        "week_start": str(week_start),
                        "schedule_id": int(row["id"]),
                        "product": row["product"],
                        "quantity": int(row["quantity"]),
                        "shift": row.get("shift", ""),
                        "day_of_week": row.get("day_of_week", ""),
                        "input_kg": 0.0,
                        "output_kg": 0.0,
                        "brand": "",
                        "tracking_number": "",
                        "loss_rate": 0.0,
                        "completed": False,
                        "completed_date": None
                    })
                if new_records:
                    supabase.table("production_records").insert(new_records).execute()
                    st.session_state['prod_record_msg'] = f"✅ 스케줄에서 {len(new_records)}건의 생산기록을 불러왔습니다!"
                    st.rerun()
            except Exception as e:
                st.error(f"❌ 불러오기 실패: {str(e)}")
                st.info("💡 Supabase SQL Editor에서 production_records 테이블을 생성해주세요.")
                st.code(
                    "CREATE TABLE IF NOT EXISTS production_records (\n"
                    "    id BIGSERIAL PRIMARY KEY,\n"
                    "    week_start TEXT NOT NULL,\n"
                    "    schedule_id BIGINT,\n"
                    "    product TEXT NOT NULL,\n"
                    "    quantity INT DEFAULT 0,\n"
                    "    shift TEXT DEFAULT '',\n"
                    "    day_of_week TEXT DEFAULT '',\n"
                    "    input_kg NUMERIC DEFAULT 0,\n"
                    "    output_kg NUMERIC DEFAULT 0,\n"
                    "    brand TEXT DEFAULT '',\n"
                    "    tracking_number TEXT DEFAULT '',\n"
                    "    loss_rate NUMERIC DEFAULT 0,\n"
                    "    completed BOOLEAN DEFAULT FALSE,\n"
                    "    completed_date TEXT,\n"
                    "    created_at TIMESTAMPTZ DEFAULT NOW()\n"
                    ");",
                    language="sql"
                )
        return

    st.divider()

    # 5) 브랜드 목록 로드
    brands = load_brands_list()

    # 6) 미완료 / 완료 분리
    incomplete = records_df[records_df["completed"] != True].copy()
    completed = records_df[records_df["completed"] == True].copy()

    # 전체 삭제 버튼
    st.markdown("---")
    col_info, col_delete = st.columns([3, 1])
    with col_info:
        st.info(f"📊 총 {len(records_df)}건 (미완료 {len(incomplete)}건 | 완료 {len(completed)}건)")
    with col_delete:
        if st.button("🗑️ 전체 삭제", key="delete_all_records", type="secondary"):
            st.session_state['confirm_delete_all'] = True
    
    # 전체 삭제 확인
    if st.session_state.get('confirm_delete_all'):
        st.warning("⚠️ 이 주차의 모든 생산기록을 삭제하시겠습니까?")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("✅ 예, 삭제합니다", key="confirm_yes", type="primary"):
                try:
                    for _, row in records_df.iterrows():
                        delete_production_record(row["id"])
                    st.session_state['prod_record_msg'] = f"✅ {len(records_df)}건의 생산기록을 모두 삭제했습니다."
                    del st.session_state['confirm_delete_all']
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 삭제 실패: {str(e)}")
        with col_no:
            if st.button("❌ 취소", key="confirm_no"):
                del st.session_state['confirm_delete_all']
                st.rerun()

    # ── 미완료 생산기록 (날짜별 그룹핑)
    if not incomplete.empty:
        st.markdown(f"### 📋 미완료 ({len(incomplete)}건)")
        st.caption("투입kg, 생산kg, 브랜드, 이력번호를 입력한 후 생산완료 버튼을 눌러주세요.")

        # 날짜별 그룹핑 (요일 기준)
        if "day_of_week" in incomplete.columns:
            incomplete["dow_sort"] = incomplete["day_of_week"].fillna("")
            # 요일 순서 정의
            day_order = {"월": 1, "화": 2, "수": 3, "목": 4, "금": 5, "토": 6, "일": 7}
            incomplete["dow_order"] = incomplete["dow_sort"].map(lambda x: day_order.get(x, 99))
            day_groups = incomplete.sort_values("dow_order").groupby("dow_sort")
            
            for dow, group_df in day_groups:
                dow_label = dow if dow else "미정"
                
                with st.expander(f"📅 {dow_label}요일 ({len(group_df)}건)", expanded=False):
                    for idx, row in group_df.iterrows():
                        rec_id = row["id"]
                        
                        # 품목별 expander (클릭하면 입력 폼이 나옴)
                        with st.expander(
                            f"🔸 {row.get('shift', '')} | **{row['product']}** - {row['quantity']}개",
                            expanded=False
                        ):
                            col1, col2 = st.columns(2)
                            with col1:
                                input_kg = st.number_input(
                                    "투입 kg", min_value=0.0, value=float(row.get("input_kg", 0) or 0),
                                    step=0.1, format="%.1f", key=f"input_kg_{rec_id}"
                                )
                                if brands:
                                    brand_options = [""] + brands
                                    current_brand = row.get("brand", "") or ""
                                    brand_idx = brand_options.index(current_brand) if current_brand in brand_options else 0
                                    brand = st.selectbox(
                                        "브랜드", options=brand_options, index=brand_idx,
                                        key=f"brand_{rec_id}"
                                    )
                                else:
                                    brand = st.text_input(
                                        "브랜드", value=row.get("brand", "") or "",
                                        key=f"brand_{rec_id}"
                                    )
                            with col2:
                                output_kg = st.number_input(
                                    "생산 kg", min_value=0.0, value=float(row.get("output_kg", 0) or 0),
                                    step=0.1, format="%.1f", key=f"output_kg_{rec_id}"
                                )
                                tracking_number = st.text_input(
                                    "이력번호", value=row.get("tracking_number", "") or "",
                                    key=f"tracking_{rec_id}"
                                )

                            # 메모
                            prod_memo = st.text_input(
                                "메모", value="", placeholder="메모 (선택사항)",
                                key=f"memo_{rec_id}"
                            )

                            # 실시간 로스율 미리보기
                            if input_kg > 0 and output_kg > 0:
                                preview_loss = input_kg - output_kg
                                preview_rate = round((preview_loss / input_kg * 100), 2)
                                if preview_loss >= 0:
                                    st.info(f"📊 로스: {preview_loss:.1f}kg | 로스율: {preview_rate}%")
                                else:
                                    st.warning(f"⚠️ 생산kg이 투입kg보다 큽니다. (차이: {abs(preview_loss):.1f}kg)")

                            # 버튼 영역
                            all_filled = input_kg > 0 and output_kg > 0 and brand and tracking_number
                            btn_col1, btn_col2 = st.columns([3, 1])
                            with btn_col1:
                                if all_filled:
                                    if st.button("✅ 생산 완료", key=f"complete_{rec_id}", type="primary"):
                                        try:
                                            loss_kg, loss_rate, comp_date = complete_production(
                                                rec_id, input_kg, output_kg, brand, tracking_number
                                            )
                                            p_code = get_product_code_by_name(row["product"])
                                            p_raw_meat = get_raw_meat_by_name(row["product"])
                                            loss_memo = prod_memo.strip() if prod_memo else ""
                                            insert_loss(
                                                comp_date,
                                                p_code,
                                                row["product"],
                                                round(loss_kg, 2),
                                                loss_memo,
                                                brand=brand,
                                                tracking_number=tracking_number,
                                                input_kg=input_kg,
                                                output_kg=output_kg,
                                                loss_rate=loss_rate,
                                                raw_meat=p_raw_meat
                                            )
                                            load_losses.clear()
                                            st.session_state['prod_record_msg'] = (
                                                f"✅ '{row['product']}' 생산 완료! "
                                                f"(로스: {loss_kg:.1f}kg, 로스율: {loss_rate}%)"
                                            )
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"❌ 처리 실패: {str(e)}")
                                else:
                                    st.button(
                                        "✅ 생산 완료", key=f"complete_disabled_{rec_id}",
                                        disabled=True, help="투입kg, 생산kg, 브랜드, 이력번호를 모두 입력해주세요."
                                    )
                            with btn_col2:
                                if st.button("🗑️ 삭제", key=f"del_rec_{rec_id}"):
                                    try:
                                        delete_production_record(rec_id)
                                        st.session_state['prod_record_msg'] = f"✅ '{row['product']}' 기록 삭제 완료"
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"❌ 삭제 실패: {str(e)}")
        else:
            # day_of_week 컬럼이 없는 경우 기존 방식
            for idx, row in incomplete.iterrows():
                rec_id = row["id"]
                with st.expander(
                    f"🔸 {row.get('day_of_week', '')} {row.get('shift', '')} | "
                    f"**{row['product']}** - {row['quantity']}개",
                    expanded=False
                ):
                    col1, col2 = st.columns(2)
                    with col1:
                        input_kg = st.number_input(
                            "투입 kg", min_value=0.0, value=float(row.get("input_kg", 0) or 0),
                            step=0.1, format="%.1f", key=f"input_kg_{rec_id}"
                        )
                        if brands:
                            brand_options = [""] + brands
                            current_brand = row.get("brand", "") or ""
                            brand_idx = brand_options.index(current_brand) if current_brand in brand_options else 0
                            brand = st.selectbox(
                                "브랜드", options=brand_options, index=brand_idx,
                                key=f"brand_{rec_id}"
                            )
                        else:
                            brand = st.text_input(
                                "브랜드", value=row.get("brand", "") or "",
                                key=f"brand_{rec_id}"
                            )
                    with col2:
                        output_kg = st.number_input(
                            "생산 kg", min_value=0.0, value=float(row.get("output_kg", 0) or 0),
                            step=0.1, format="%.1f", key=f"output_kg_{rec_id}"
                        )
                        tracking_number = st.text_input(
                            "이력번호", value=row.get("tracking_number", "") or "",
                            key=f"tracking_{rec_id}"
                        )

                    # 메모
                    prod_memo = st.text_input(
                        "메모", value="", placeholder="메모 (선택사항)",
                        key=f"memo_{rec_id}"
                    )

                    # 실시간 로스율 미리보기
                    if input_kg > 0 and output_kg > 0:
                        preview_loss = input_kg - output_kg
                        preview_rate = round((preview_loss / input_kg * 100), 2)
                        if preview_loss >= 0:
                            st.info(f"📊 로스: {preview_loss:.1f}kg | 로스율: {preview_rate}%")
                        else:
                            st.warning(f"⚠️ 생산kg이 투입kg보다 큽니다. (차이: {abs(preview_loss):.1f}kg)")

                    # 버튼 영역
                    all_filled = input_kg > 0 and output_kg > 0 and brand and tracking_number
                    btn_col1, btn_col2 = st.columns([3, 1])
                    with btn_col1:
                        if all_filled:
                            if st.button("✅ 생산 완료", key=f"complete_{rec_id}", type="primary"):
                                try:
                                    loss_kg, loss_rate, comp_date = complete_production(
                                        rec_id, input_kg, output_kg, brand, tracking_number
                                    )
                                    p_code = get_product_code_by_name(row["product"])
                                    p_raw_meat = get_raw_meat_by_name(row["product"])
                                    loss_memo = prod_memo.strip() if prod_memo else ""
                                    insert_loss(
                                        comp_date,
                                        p_code,
                                        row["product"],
                                        round(loss_kg, 2),
                                        loss_memo,
                                        brand=brand,
                                        tracking_number=tracking_number,
                                        input_kg=input_kg,
                                        output_kg=output_kg,
                                        loss_rate=loss_rate,
                                        raw_meat=p_raw_meat
                                    )
                                    load_losses.clear()
                                    st.session_state['prod_record_msg'] = (
                                        f"✅ '{row['product']}' 생산 완료! "
                                        f"(로스: {loss_kg:.1f}kg, 로스율: {loss_rate}%)"
                                    )
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 처리 실패: {str(e)}")
                        else:
                            st.button(
                                "✅ 생산 완료", key=f"complete_disabled_{rec_id}",
                                disabled=True, help="투입kg, 생산kg, 브랜드, 이력번호를 모두 입력해주세요."
                            )
                    with btn_col2:
                        if st.button("🗑️ 삭제", key=f"del_rec_{rec_id}"):
                            try:
                                delete_production_record(rec_id)
                                st.session_state['prod_record_msg'] = f"✅ '{row['product']}' 기록 삭제 완료"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 삭제 실패: {str(e)}")

    else:
        st.success("🎉 모든 생산이 완료되었습니다!")

    # ── 완료된 생산기록 (날짜별 그룹핑)
    if not completed.empty:
        st.divider()
        st.markdown(f"### ✅ 완료 ({len(completed)}건)")

        # 날짜별로 그룹핑
        if "completed_date" in completed.columns:
            completed["comp_date_sort"] = completed["completed_date"].fillna("")
            date_groups = completed.groupby("comp_date_sort")
            
            for comp_date, group_df in sorted(date_groups, key=lambda x: x[0], reverse=True):
                date_label = comp_date if comp_date else "날짜 미정"
                
                with st.expander(f"📅 {date_label} ({len(group_df)}건)", expanded=False):
                    # 테이블로 표시
                    display_cols = []
                    col_map = {}
                    for col, label in [
                        ("day_of_week", "요일"), ("shift", "교대"),
                        ("product", "제품"), ("quantity", "수량"),
                        ("input_kg", "투입kg"), ("output_kg", "생산kg"),
                        ("loss_rate", "로스율(%)"), ("brand", "브랜드"),
                        ("tracking_number", "이력번호")
                    ]:
                        if col in group_df.columns:
                            display_cols.append(col)
                            col_map[col] = label

                    display_df = group_df[display_cols].rename(columns=col_map)
                    st.dataframe(display_df, use_container_width=True, hide_index=True)
                    
                    # 이 날짜의 기록 삭제
                    st.markdown("##### 🗑️ 기록 삭제")
                    delete_options = group_df.apply(
                        lambda r: f"{r.get('day_of_week', '')} {r.get('shift', '')} | {r['product']} - {r.get('output_kg', 0)}kg",
                        axis=1
                    ).tolist()
                    delete_idx = st.selectbox(
                        "삭제할 기록", options=range(len(delete_options)),
                        format_func=lambda i: delete_options[i],
                        index=None, placeholder="선택...", key=f"del_comp_{comp_date}"
                    )
                    if delete_idx is not None:
                        if st.button("🗑️ 삭제", key=f"btn_del_{comp_date}_{delete_idx}"):
                            try:
                                rec_id = group_df.iloc[delete_idx]["id"]
                                delete_production_record(rec_id)
                                st.session_state['prod_record_msg'] = "✅ 완료 기록 삭제 완료"
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 삭제 실패: {str(e)}")


# ========================
# 로스 현황
# ========================


# ========================
# 로스 현황 - 데이터 전처리
# ========================

def _prepare_loss_df():
    """로스 데이터를 로드하고 전처리하여 반환 (공통 로직)"""
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
        meat_origin_map_edit = {}
        if not raw_meats_df_edit.empty:
            for _, rm in raw_meats_df_edit.iterrows():
                name = str(rm.get("name", "")).strip()
                origin = str(rm.get("origin", "")).strip()
                if name:
                    meat_origin_map_edit[name] = origin
        raw_meat_edit_options = []
        for name, origin in meat_origin_map_edit.items():
            if origin:
                raw_meat_edit_options.append(f"{name} ({origin})")
            else:
                raw_meat_edit_options.append(name)
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

    current_raw_meat = str(row.get("raw_meat", "")).strip()
    raw_meat_all_options = [""] + raw_meat_edit_options
    raw_meat_default_idx = 0
    for i, opt in enumerate(raw_meat_all_options):
        if opt.startswith(current_raw_meat) and current_raw_meat:
            raw_meat_default_idx = i
            break
    edit_raw_meat_sel = st.selectbox("사용원육", options=raw_meat_all_options, index=raw_meat_default_idx, key=f"edit_rawmeat_{rid}")
    edit_raw_meat = edit_raw_meat_sel.split(" (")[0].strip() if edit_raw_meat_sel else ""

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

                memo_parts = []
                if edit_brand:
                    memo_parts.append(f"브랜드:{edit_brand}")
                if edit_tracking:
                    memo_parts.append(f"이력번호:{edit_tracking}")
                memo_parts.append(f"투입:{edit_input_kg}kg")
                if edit_output_kg > 0:
                    memo_parts.append(f"생산:{edit_output_kg}kg")
                if edit_memo:
                    memo_parts.append(edit_memo)
                full_memo = " | ".join(memo_parts)

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
                    "memo": full_memo,
                }
                if new_loss_rate is not None:
                    update_data["loss_rate"] = new_loss_rate

                update_loss(rid, update_data)
                load_losses.clear()
                rate_str = f" (로스율: {new_loss_rate}%)" if new_loss_rate is not None else ""
                st.session_state["_loss_edit_success"] = f"✅ '{p_name}' 수정 완료!{rate_str}"
                st.rerun()
            except Exception as e:
                st.error(f"❌ 수정 실패: {str(e)}")
    with col_btn2:
        if st.button("🗑️ 삭제", key=f"edit_del_{rid}"):
            try:
                delete_loss(int(rid))
                load_losses.clear()
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

    # ── 전체 요약 메트릭
    total_rates = df["loss_rate"].dropna()
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    with col_m1:
        st.metric("총 건수", f"{len(df)}건")
    with col_m2:
        st.metric("총 로스", f"{df['weight_kg'].sum():,.1f}kg")
    with col_m3:
        st.metric("평균 로스율", f"{total_rates.mean():.1f}%" if not total_rates.empty else "-")
    with col_m4:
        st.metric("최고 로스율", f"{total_rates.max():.1f}%" if not total_rates.empty else "-")

    st.divider()

    # ── 필터: 제품 / 원육 / 브랜드
    with st.expander("🔍 필터 (제품 / 원육 / 브랜드)", expanded=False):
        col_f1, col_f2, col_f3 = st.columns(3)
        with col_f1:
            products_list = sorted(df["product_name"].fillna("").astype(str).str.strip().unique().tolist())
            products_list = [p for p in products_list if p]
            selected_product = st.selectbox("📦 제품", options=["전체"] + products_list, index=0, key="loss_product_filter")
        with col_f2:
            unique_meats = sorted([m for m in df["raw_meat"].unique().tolist() if m])
            selected_meat = st.selectbox("🥩 원육", options=["전체"] + unique_meats, index=0, key="loss_meat_filter")
        with col_f3:
            unique_brands = sorted([b for b in df["brand"].unique().tolist() if b])
            selected_brand = st.selectbox("🏷️ 브랜드", options=["전체"] + unique_brands, index=0, key="loss_brand_filter")

    if selected_product != "전체":
        df = df[df["product_name"].fillna("").astype(str).str.strip() == selected_product]
    if selected_meat != "전체":
        df = df[df["raw_meat"] == selected_meat]
    if selected_brand != "전체":
        df = df[df["brand"] == selected_brand]

    if df.empty:
        st.warning("필터 조건에 맞는 데이터가 없습니다.")
        return

    # ══════════════════════════════════════
    # 월 선택 → 일별 상세 → 월별 요약 순서
    # ══════════════════════════════════════

    if "month" not in df.columns or df["month"].isna().all():
        st.warning("날짜 데이터가 없어 월별 분류를 할 수 없습니다.")
        return

    months_sorted = sorted(df["month"].dropna().unique().tolist(), reverse=True)

    # ── 월 선택
    month_labels = [f"📅 {m} ({len(df[df['month'] == m])}건)" for m in months_sorted]
    selected_month_idx = st.selectbox(
        "**월 선택**",
        options=range(len(months_sorted)),
        format_func=lambda i: month_labels[i],
        index=0, key="loss_month_selector"
    )
    selected_month = months_sorted[selected_month_idx]
    month_df = df[df["month"] == selected_month].copy()

    # 선택 월 메트릭
    m_rates = month_df["loss_rate"].dropna()
    col_s1, col_s2, col_s3 = st.columns(3)
    with col_s1:
        st.metric(f"{selected_month} 건수", f"{len(month_df)}건")
    with col_s2:
        st.metric("로스 합계", f"{month_df['weight_kg'].sum():,.1f}kg")
    with col_s3:
        st.metric("평균 로스율", f"{m_rates.mean():.1f}%" if not m_rates.empty else "-")

    st.divider()

    # ── 일별 상세 (expander 드릴다운)
    st.markdown(f"### 📝 {selected_month} 일별 상세")
    st.caption("날짜를 클릭하면 상세 데이터와 수정/삭제 기능을 사용할 수 있습니다.")

    dates_in_month = sorted(month_df["loss_date"].dropna().unique().tolist(), reverse=True)

    for d in dates_in_month:
        d_df = month_df[month_df["loss_date"] == d].copy()
        d_rates = d_df["loss_rate"].dropna()
        avg_str = f" | 평균 로스율: {d_rates.mean():.1f}%" if not d_rates.empty else ""
        loss_sum = d_df["weight_kg"].sum()

        with st.expander(f"📅 **{d}** — {len(d_df)}건 | 로스: {loss_sum:,.1f}kg{avg_str}"):
            detail_cols = ["product_name", "raw_meat", "brand", "tracking_number",
                           "input_kg", "output_kg", "weight_kg", "loss_rate", "memo_clean"]
            detail_cols = [c for c in detail_cols if c in d_df.columns]
            detail_names = {
                "product_name": "제품명", "raw_meat": "원육", "brand": "브랜드",
                "tracking_number": "이력번호", "input_kg": "투입(kg)",
                "output_kg": "생산(kg)", "weight_kg": "로스(kg)",
                "loss_rate": "로스율(%)", "memo_clean": "메모"
            }
            st.dataframe(d_df[detail_cols].rename(columns=detail_names),
                         use_container_width=True, hide_index=True)

            st.markdown("##### ✏️ 수정 / 🗑️ 삭제")
            for _, row in d_df.iterrows():
                rid = row["id"]
                label_str = f"{row.get('product_name', '')} | {row.get('brand', '')} | 로스: {row.get('weight_kg', 0)}kg"
                with st.expander(f"🔸 {label_str}", expanded=False):
                    _render_loss_edit_form(row, rid)

    # ── 월별 로스 요약
    st.divider()
    st.markdown("### 📅 월별 로스 요약")

    monthly_summary = []
    for m in months_sorted:
        m_df = df[df["month"] == m]
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

    # ── 생산kg 미입력 건
    incomplete = df[(df["output_kg"].fillna(0).astype(float) == 0) | (df["output_kg"].isna())]
    if not incomplete.empty:
        st.divider()
        st.subheader("⚠️ 생산kg 미입력 건")
        st.caption("생산kg이 입력되지 않은 건입니다. 여기서 바로 수정할 수 있습니다.")
        for _, row in incomplete.iterrows():
            rid = row["id"]
            label = f"[{row.get('loss_date', '')}] {row.get('product_name', '')} | 투입: {row.get('input_kg', 0)}kg"
            with st.expander(label):
                new_output = st.number_input("생산 kg", min_value=0.0, value=0.0, step=0.1, key=f"loss_output_edit_{rid}")
                if st.button("💾 저장", key=f"loss_output_save_{rid}"):
                    if new_output <= 0:
                        st.error("생산 kg을 입력해주세요.")
                    else:
                        try:
                            input_kg_val = float(row.get("input_kg", 0))
                            new_loss_rate = round((input_kg_val - new_output) / input_kg_val * 100, 2) if input_kg_val > 0 else 0
                            new_weight = round(input_kg_val - new_output, 2)
                            old_memo = str(row.get("memo", "")) if row.get("memo") else ""
                            if "생산:" not in old_memo:
                                new_memo = old_memo + f" | 생산:{new_output}kg" if old_memo else f"생산:{new_output}kg"
                            else:
                                new_memo = old_memo
                            supabase.table("losses").update({
                                "output_kg": new_output, "loss_rate": new_loss_rate,
                                "weight_kg": new_weight, "memo": new_memo,
                            }).eq("id", rid).execute()
                            load_losses.clear()
                            st.success(f"✅ 저장 완료! 로스율: {new_loss_rate}%")
                            st.rerun()
                        except Exception as e:
                            st.error(f"❌ 저장 실패: {str(e)}")

    # ── 일괄 삭제
    st.divider()
    st.subheader("🗑️ 로스 일괄 삭제")

    if st.session_state.get("_loss_delete_success"):
        st.success(st.session_state["_loss_delete_success"])
        del st.session_state["_loss_delete_success"]

    if not df.empty:
        df = df.reset_index(drop=True)
        id_list = df["id"].tolist()
        label_list = df.apply(
            lambda r: f"[{r.get('loss_date', '')}] {r.get('product_name', '')} - {r.get('weight_kg', 0)}kg", axis=1
        ).tolist()
        id_label_map = {str(rid): label for rid, label in zip(id_list, label_list)}
        all_ids = [str(rid) for rid in id_list]

        if "loss_delete_targets" in st.session_state:
            valid = [v for v in st.session_state["loss_delete_targets"] if v in all_ids]
            st.session_state["loss_delete_targets"] = valid
        if st.session_state.get("_loss_select_all_flag"):
            st.session_state["loss_delete_targets"] = list(all_ids)
            del st.session_state["_loss_select_all_flag"]
        if st.session_state.get("_loss_deselect_all_flag"):
            st.session_state["loss_delete_targets"] = []
            del st.session_state["_loss_deselect_all_flag"]

        col_sel1, col_sel2 = st.columns([1, 1])
        with col_sel1:
            if st.button("✅ 전체 선택", key="loss_select_all_btn"):
                st.session_state["_loss_select_all_flag"] = True
                st.rerun()
        with col_sel2:
            if st.button("❌ 선택 해제", key="loss_deselect_all_btn"):
                st.session_state["_loss_deselect_all_flag"] = True
                st.rerun()

        selected_ids = st.multiselect(
            "삭제할 로스 선택 (여러 개 선택 가능)",
            options=all_ids,
            format_func=lambda x: id_label_map.get(x, x),
            key="loss_delete_targets", placeholder="로스를 선택하세요..."
        )

        if selected_ids:
            st.caption(f"🔴 {len(selected_ids)}건 선택됨")
            col_a, col_b = st.columns([1, 4])
            with col_a:
                if st.button(f"🗑️ {len(selected_ids)}건 삭제", type="primary", key="loss_delete_btn"):
                    delete_count = 0
                    for rid in selected_ids:
                        try:
                            delete_loss(int(rid))
                            delete_count += 1
                        except Exception as e:
                            st.error(f"삭제 실패: {str(e)}")
                    if delete_count > 0:
                        load_losses.clear()
                        st.session_state["_loss_deselect_all_flag"] = True
                        st.session_state["_loss_delete_success"] = f"✅ {delete_count}건 삭제 완료"
                        st.rerun()

# ========================
# 생산 등록 (로스 + 생산기록)
# ========================

def _show_production_form():
    st.subheader("생산 등록")

    # 성공 메시지 표시
    if 'prod_form_msg' in st.session_state:
        st.success(st.session_state['prod_form_msg'])
        try:
            st.toast(st.session_state['prod_form_msg'], icon="✅")
        except:
            pass
        del st.session_state['prod_form_msg']

    _show_production_record_form()


def _show_production_record_form():
    """생산기록에 제품 추가"""
    st.markdown("#### 생산기록에 제품 추가")
    
    products_df = load_products()
    weeks = get_schedule_weeks()
    
    if not weeks:
        st.warning("저장된 주차가 없습니다. 스케줄을 먼저 생성해주세요.")
        return
    
    # 주차 선택
    week_options = [f"{w[0]} ~ {w[1]}" for w in weeks]
    selected_week = st.selectbox("📅 주차 선택", week_options, key="prod_add_week")
    week_start = weeks[week_options.index(selected_week)][0]

    # 해당 주차 스케줄에서 요일 라벨 가져오기 (예: "02/02 (월)")
    schedule_df = load_schedule_products(week_start)
    if not schedule_df.empty and "day_of_week" in schedule_df.columns:
        day_labels = schedule_df["day_of_week"].drop_duplicates().tolist()
        day_labels = sorted(day_labels, key=lambda x: x)  # 날짜순 정렬
    else:
        day_labels = ["월", "화", "수", "목", "금"]

    with st.form("production_record_form"):
        # 제품 선택
        if not products_df.empty:
            product_options = products_df.apply(
                lambda r: f"{r['product_code']} - {r['product_name']}", axis=1
            ).tolist()
            selected_product = st.selectbox(
                "제품 선택", options=product_options, index=None,
                placeholder="제품을 선택하세요..."
            )
        else:
            selected_product = None
            st.warning("등록된 제품이 없습니다. 제품 탭에서 먼저 등록해주세요.")
        
        col1, col2 = st.columns(2)
        with col1:
            day_of_week = st.selectbox("요일", day_labels, index=0)
            quantity = st.number_input("수량 (개)", min_value=1, value=1)
        with col2:
            shift = st.selectbox("교대", ["주간", "야간"], index=0)
        
        submitted = st.form_submit_button("💾 추가", type="primary")
        
        if submitted:
            if not selected_product:
                st.error("제품을 선택해주세요.")
            else:
                try:
                    product_name = selected_product.split(" - ", 1)[1] if " - " in selected_product else selected_product
                    
                    new_record = {
                        "week_start": str(week_start),
                        "schedule_id": None,
                        "product": product_name,
                        "quantity": int(quantity),
                        "shift": shift,
                        "day_of_week": day_of_week,
                        "input_kg": 0.0,
                        "output_kg": 0.0,
                        "brand": "",
                        "tracking_number": "",
                        "loss_rate": 0.0,
                        "completed": False,
                        "completed_date": None
                    }
                    
                    insert_production_record(new_record)
                    st.session_state['prod_form_msg'] = f"✅ '{product_name}' 생산기록 추가 완료!"
                    st.rerun()
                    
                except Exception as e:
                    st.error(f"❌ 추가 실패: {str(e)}")


def _show_loss_form():
    """로스 등록 (제품명/사용원육/브랜드/이력번호/투입kg/생산kg/메모)"""
    st.markdown("#### 📌 로스 등록")

    # 등록 성공 알림
    if st.session_state.get("_loss_reg_success"):
        st.success(st.session_state["_loss_reg_success"])
        st.toast(st.session_state["_loss_reg_success"])
        del st.session_state["_loss_reg_success"]

    products_df = load_products()
    brands = load_brands_list()

    # 원육 목록 로드 (원산지 포함)
    from views.products.rawmeat_tab import load_raw_meats
    raw_meats_df = load_raw_meats()
    # 원육명 → 원산지 매핑
    meat_origin_map = {}
    if not raw_meats_df.empty:
        for _, rm in raw_meats_df.iterrows():
            name = str(rm.get("name", "")).strip()
            origin = str(rm.get("origin", "")).strip()
            if name:
                meat_origin_map[name] = origin

    # 원육(원산지) 선택 옵션 생성
    raw_meat_options = []
    for name, origin in meat_origin_map.items():
        if origin:
            raw_meat_options.append(f"{name} ({origin})")
        else:
            raw_meat_options.append(name)

    # 제품 선택
    if not products_df.empty:
        product_options = products_df.apply(
            lambda r: f"{r['product_code']} | {r['product_name']}", axis=1
        ).tolist()
        selected_product = st.selectbox(
            "제품명", options=product_options, index=None,
            placeholder="제품을 선택하세요...", key="loss_reg_product"
        )
    else:
        selected_product = None
        st.warning("등록된 제품이 없습니다. 제품 탭에서 먼저 등록해주세요.")

    # 제품 변경 감지 → 원육 자동 변경
    prev_product = st.session_state.get("_loss_reg_prev_product", None)
    if selected_product != prev_product:
        st.session_state["_loss_reg_prev_product"] = selected_product
        # 제품이 변경되면 원육 selectbox 값을 업데이트
        if selected_product:
            p_name = selected_product.split(" | ", 1)[1] if " | " in selected_product else ""
            default_raw_meat = get_raw_meat_by_name(p_name)
            # 매칭되는 옵션 찾기
            matched_option = ""
            for opt in raw_meat_options:
                if opt.startswith(default_raw_meat) and default_raw_meat:
                    matched_option = opt
                    break
            st.session_state["loss_reg_rawmeat"] = matched_option
        else:
            st.session_state["loss_reg_rawmeat"] = ""

    # 사용원육: 수정 가능한 selectbox
    raw_meat_selection = st.selectbox(
        "사용원육 (원산지)", options=[""] + raw_meat_options,
        key="loss_reg_rawmeat"
    )
    # 원육명만 추출 (원산지 제거)
    raw_meat = raw_meat_selection.split(" (")[0].strip() if raw_meat_selection else ""

    col1, col2 = st.columns(2)
    with col1:
        brand = st.selectbox("브랜드", options=[""] + brands, index=0,
                             placeholder="브랜드 선택...", key="loss_reg_brand")
    with col2:
        tracking_number = st.text_input("이력번호", placeholder="이력번호 입력", key="loss_reg_tracking")

    col3, col4 = st.columns(2)
    with col3:
        input_kg = st.number_input("투입 kg", min_value=0.0, value=0.0, step=0.1, key="loss_reg_input_kg")
    with col4:
        output_kg = st.number_input("생산 kg", min_value=0.0, value=0.0, step=0.1, key="loss_reg_output_kg")

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

    memo = st.text_input("메모", placeholder="메모 (선택)", key="loss_reg_memo")

    loss_date = st.date_input("날짜", value=date.today(), key="loss_reg_date")

    if st.button("💾 로스 등록", type="primary", use_container_width=True):
        if not selected_product:
            st.error("제품을 선택해주세요.")
        elif not brand:
            st.error("브랜드를 선택해주세요.")
        elif not tracking_number.strip():
            st.error("이력번호를 입력해주세요.")
        elif input_kg <= 0:
            st.error("투입 kg을 입력해주세요.")
        else:
            p_code = selected_product.split(" | ")[0].strip()
            p_name = selected_product.split(" | ", 1)[1].strip() if " | " in selected_product else ""
            loss_rate = round((input_kg - output_kg) / input_kg * 100, 2) if input_kg > 0 and output_kg > 0 else None
            weight_kg = round(input_kg - output_kg, 2) if output_kg > 0 else 0

            # 메모에 상세 정보 추가
            memo_parts = []
            if brand:
                memo_parts.append(f"브랜드:{brand}")
            if tracking_number:
                memo_parts.append(f"이력번호:{tracking_number}")
            memo_parts.append(f"투입:{input_kg}kg")
            if output_kg > 0:
                memo_parts.append(f"생산:{output_kg}kg")
            if memo:
                memo_parts.append(memo)
            full_memo = " | ".join(memo_parts)

            try:
                insert_loss(
                    loss_date=loss_date,
                    product_code=p_code,
                    product_name=p_name,
                    weight_kg=weight_kg,
                    memo=full_memo,
                    brand=brand,
                    tracking_number=tracking_number,
                    input_kg=input_kg,
                    output_kg=output_kg,
                    loss_rate=loss_rate,
                    raw_meat=raw_meat,
                )
                load_losses.clear()
                if loss_rate is not None:
                    st.session_state["_loss_reg_success"] = f"✅ '{p_name}' 로스 등록 완료! (로스율: {loss_rate}%)"
                else:
                    st.session_state["_loss_reg_success"] = f"✅ '{p_name}' 로스 등록 완료! (생산kg 미입력)"
                st.rerun()
            except Exception as e:
                st.error(f"❌ 등록 실패: {str(e)}")


# ========================
# 로스 분석
# ========================

def _show_loss_analysis():
    st.subheader("📊 로스 분석")

    df = load_losses()

    if df.empty:
        st.info("등록된 로스 데이터가 없습니다.")
        return

    # 로스율 계산
    def calc_loss_rate(row):
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        # loss_rate 컬럼이 있으면 우선 사용
        if pd.notna(row.get("loss_rate")) and row.get("loss_rate") not in [None, 0, 0.0, ""]:
            return float(row["loss_rate"])
        if "투입:" in memo_str and "생산:" in memo_str:
            try:
                input_kg = float(memo_str.split("투입:")[1].split("kg")[0].strip())
                output_kg = float(memo_str.split("생산:")[1].split("kg")[0].strip())
                if input_kg > 0:
                    return round((input_kg - output_kg) / input_kg * 100, 2)
            except:
                pass
        return None

    df["loss_rate_calc"] = df.apply(calc_loss_rate, axis=1)

    # 원육 정보 조인
    products_df = load_products()
    if not products_df.empty and "product_name" in df.columns:
        product_meat_map = dict(zip(
            products_df["product_name"].astype(str).str.strip(),
            products_df["used_raw_meat"].fillna("").astype(str).str.strip()
        ))
        df["raw_meat"] = df["product_name"].map(product_meat_map).fillna("")
    else:
        df["raw_meat"] = ""

    # 날짜 변환
    if "loss_date" in df.columns:
        df["loss_date_dt"] = pd.to_datetime(df["loss_date"], errors="coerce")

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
