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

def _show_loss_list():
    st.subheader("로스 현황")

    df = load_losses()

    if df.empty:
        st.info("등록된 로스 데이터가 없습니다.")
        return

    # 원육 정보: losses DB의 raw_meat 우선, 없으면 products 테이블에서 조인
    if "raw_meat" not in df.columns:
        df["raw_meat"] = ""
    df["raw_meat"] = df["raw_meat"].fillna("").astype(str).str.strip()

    # raw_meat가 비어있는 행만 products에서 보충
    products_df = load_products()
    if not products_df.empty and "product_name" in df.columns:
        product_meat_map = dict(zip(
            products_df["product_name"].astype(str).str.strip(),
            products_df["used_raw_meat"].fillna("").astype(str).str.strip()
        ))
        empty_mask = df["raw_meat"] == ""
        df.loc[empty_mask, "raw_meat"] = df.loc[empty_mask, "product_name"].map(product_meat_map).fillna("")

    # memo에서 브랜드 추출 (기존 데이터 호환) + DB 컬럼 우선
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

    # 기존 데이터 호환: brand 컬럼이 비어있으면 memo에서 추출
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

    # 기존 데이터 호환: loss_rate가 없으면 memo에서 추출
    def extract_loss_rate(row):
        if pd.notna(row.get("loss_rate")) and row.get("loss_rate") not in [None, 0, 0.0, ""]:
            rate = float(row["loss_rate"])
            # 소수 형태(0.0369)로 저장된 경우 백분율로 변환
            if 0 < rate < 1:
                rate = round(rate * 100, 2)
            return rate
        memo_str = str(row.get("memo", "")) if row.get("memo") else ""
        if "투입:" in memo_str and "생산:" in memo_str:
            try:
                input_part = memo_str.split("투입:")[1].split("kg")[0].strip()
                output_part = memo_str.split("생산:")[1].split("kg")[0].strip()
                input_kg = float(input_part)
                output_kg = float(output_part)
                if input_kg > 0:
                    return round((input_kg - output_kg) / input_kg * 100, 2)
            except:
                pass
        return None
    df["loss_rate"] = df.apply(extract_loss_rate, axis=1)

    # 기존 데이터 호환: memo에서 순수 메모만 남기기 (이력번호/브랜드/투입 정보 제거)
    def clean_memo(memo):
        memo_str = str(memo).strip() if memo else ""
        if "이력번호:" in memo_str and "브랜드:" in memo_str:
            return ""
        return memo_str
    df["memo_clean"] = df["memo"].apply(clean_memo)

    # 전체 데이터 기준 메트릭
    col1, col2 = st.columns(2)
    with col1:
        st.metric("총 로스 건수", f"{len(df)}건")
    with col2:
        rates = df["loss_rate"].dropna()
        if not rates.empty:
            st.metric("평균 로스율", f"{rates.mean():.1f}%")

    st.divider()

    # 필터 (2행 2열)
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        if "loss_date" in df.columns and df["loss_date"].notna().any():
            dates = sorted(df["loss_date"].unique().tolist(), reverse=True)
            selected_date = st.selectbox(
                "📅 날짜", options=["전체"] + dates, index=0, key="loss_date_filter"
            )
            if selected_date != "전체":
                df = df[df["loss_date"] == selected_date]
    with col_f2:
        if "product_name" in df.columns:
            products = sorted(df["product_name"].fillna("").astype(str).str.strip().unique().tolist())
            products = [p for p in products if p]
            if products:
                selected_product = st.selectbox(
                    "📦 제품", options=["전체"] + products, index=0, key="loss_product_filter"
                )
                if selected_product != "전체":
                    df = df[df["product_name"].fillna("").astype(str).str.strip() == selected_product]

    col_f3, col_f4 = st.columns(2)
    with col_f3:
        unique_meats = sorted(df["raw_meat"].unique().tolist())
        unique_meats = [m for m in unique_meats if m]
        if unique_meats:
            selected_meat = st.selectbox(
                "🥩 원육", options=["전체"] + unique_meats, index=0, key="loss_meat_filter"
            )
            if selected_meat != "전체":
                df = df[df["raw_meat"] == selected_meat]
    with col_f4:
        unique_brands = sorted(df["brand"].unique().tolist())
        unique_brands = [b for b in unique_brands if b]
        if unique_brands:
            selected_brand = st.selectbox(
                "🏷️ 브랜드", options=["전체"] + unique_brands, index=0, key="loss_brand_filter"
            )
            if selected_brand != "전체":
                df = df[df["brand"] == selected_brand]

    # 필터 후 메트릭
    if len(df) > 0:
        rates = df["loss_rate"].dropna()
        avg_rate = f" | 평균 로스율: {rates.mean():.1f}%" if not rates.empty else ""
        st.caption(f"📊 필터 결과: {len(df)}건{avg_rate}")

    # 테이블
    display_cols = ["loss_date", "product_name", "loss_rate", "raw_meat", "brand", "tracking_number", "input_kg", "output_kg", "memo_clean"]
    display_cols = [c for c in display_cols if c in df.columns]
    col_names = {
        "loss_date": "날짜", "product_name": "제품명",
        "raw_meat": "원육", "loss_rate": "로스율(%)",
        "brand": "브랜드", "tracking_number": "이력번호",
        "input_kg": "투입(kg)", "output_kg": "생산(kg)",
        "memo_clean": "메모"
    }
    display_df = df[display_cols].rename(columns=col_names)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # 삭제
    st.divider()
    st.subheader("🗑️ 로스 삭제")

    # 삭제 성공 메시지
    if st.session_state.get("_loss_delete_success"):
        st.success(st.session_state["_loss_delete_success"])
        del st.session_state["_loss_delete_success"]

    if not df.empty:
        df = df.reset_index(drop=True)

        # id와 라벨을 매핑
        id_list = df["id"].tolist()
        label_list = df.apply(
            lambda r: f"[{r.get('loss_date', '')}] {r.get('product_name', '')} - {r.get('weight_kg', 0)}kg",
            axis=1
        ).tolist()
        id_label_map = {str(rid): label for rid, label in zip(id_list, label_list)}

        all_ids = [str(rid) for rid in id_list]

        # session_state 정리: options에 없는 값 제거 (삭제 후 rerun 시 잔여 ID 방지)
        if "loss_delete_targets" in st.session_state:
            valid = [v for v in st.session_state["loss_delete_targets"] if v in all_ids]
            st.session_state["loss_delete_targets"] = valid

        # 전체 선택 / 해제를 별도 session_state 플래그로 처리
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
            key="loss_delete_targets",
            placeholder="로스를 선택하세요..."
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
                        # 위젯 렌더링 후이므로 직접 수정 불가 → 플래그로 처리
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
    """로스 등록"""
    st.markdown("#### 로스 등록")

    products_df = load_products()

    with st.form("loss_form"):
        loss_date = st.date_input("날짜", value=date.today(), key="loss_date_input")

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

        weight_kg = st.number_input("중량 (kg)", min_value=0.0, value=0.0, step=0.1)
        memo = st.text_area("메모", placeholder="추가 메모...", height=80)

        submitted = st.form_submit_button("💾 등록", type="primary")

        if submitted:
            if not selected_product:
                st.error("제품을 선택해주세요.")
            elif weight_kg == 0.0:
                st.error("중량을 입력해주세요.")
            else:
                p_code = selected_product.split(" - ")[0]
                p_name = selected_product.split(" - ", 1)[1] if " - " in selected_product else ""
                insert_loss(loss_date, p_code, p_name, weight_kg, memo)
                load_losses.clear()
                st.success(f"✅ 로스 등록 완료!")
                st.rerun()


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
