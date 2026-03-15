import streamlit as st
import pandas as pd
import math
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import os
from utils.auth import get_supabase_client, is_authenticated, can_edit

# ========================
# Supabase 연결
# ========================

supabase = get_supabase_client()

# ========================
# 설정 상수
# ========================

DAYS = ["월", "화", "수", "목", "금"]
DAILY_LIMIT = 200
# 요일·교대별 생산량 상한 {요일: {교대: 상한}}
SHIFT_LIMITS = {
    "월": {"주간": 100, "야간": 150},
    "화": {"주간": 200, "야간": 200},
    "수": {"주간": 200, "야간": 200},
    "목": {"주간": 200, "야간": 200},
    "금": {"주간": 200, "야간": 200},
}

def get_shift_limit(day, shift):
    """요일·교대별 생산 상한 반환"""
    return SHIFT_LIMITS.get(day, {}).get(shift, DAILY_LIMIT)
WORK_HOURS = 8 * 60 * 60
BATCH_SIZE = 1

# 안전재고 설정: 특정 제품코드별 최소 유지 재고량
SAFETY_STOCK = {
    "F0000047": 300,
    "F0000048": 200,
    "F0000050": 200,
    "F0000078": 200,
}

# 특수 제약 제품: 하루에 이 그룹 중 1품목만 생산 가능, 월요일은 야간만
EXCLUSIVE_PRODUCTS = {"F0000047", "F0000048", "F0000050", "F0000078"}

# 생산량 집계 제외 제품: daily_sum에 포함하지 않아 교대별 상한에 영향 안 줌
EXCLUDE_FROM_LIMIT = {"E0000072", "E0000073"}

# ========================
# 유틸리티 함수
# ========================

def get_week_monday(selected_date):
    weekday = selected_date.weekday()
    days_to_monday = weekday
    monday = selected_date - timedelta(days=days_to_monday)
    return monday

def get_allowed_shifts(production_timing):
    timing = str(production_timing).strip() if production_timing else "주야"
    if timing == "주":
        return ['주간']
    elif timing == "야":
        return ['야간']
    return ['주간', '야간']

def get_urgency(reason, product, deadline_days, is_next_week):
    urgency = 0
    if "2일치 부족" in reason:
        urgency += 80
    if is_next_week or "다음주" in reason:
        urgency -= 30
    if "안전재고" in reason and "2일치" not in reason:
        urgency += 20
    if deadline_days <= 0:
        urgency += 60
    elif deadline_days == 1:
        urgency += 30
    return urgency

# ========================
# 초성 검색 유틸리티
# ========================

CHOSUNG_LIST = [
    'ㄱ', 'ㄲ', 'ㄴ', 'ㄷ', 'ㄸ', 'ㄹ', 'ㅁ', 'ㅂ', 'ㅃ', 'ㅅ',
    'ㅆ', 'ㅇ', 'ㅈ', 'ㅉ', 'ㅊ', 'ㅋ', 'ㅌ', 'ㅍ', 'ㅎ'
]

def get_chosung(char):
    """한글 한 글자의 초성 반환"""
    if '가' <= char <= '힣':
        code = ord(char) - ord('가')
        return CHOSUNG_LIST[code // 588]
    return char

def get_chosung_string(text):
    """문자열의 초성 추출"""
    return ''.join(get_chosung(c) for c in text)

def is_chosung_only(text):
    """입력이 초성만으로 이루어져 있는지 확인"""
    chosung_set = set(CHOSUNG_LIST)
    return all(c in chosung_set for c in text if c.strip())

def match_chosung(query, target):
    """초성 검색 매칭 - query가 target의 초성에 포함되는지"""
    if not query:
        return True
    query_lower = query.lower().strip()
    target_lower = target.lower().strip()
    
    # 일반 텍스트 포함 검색
    if query_lower in target_lower:
        return True
    
    # 초성 검색
    if is_chosung_only(query_lower):
        target_chosung = get_chosung_string(target_lower)
        if query_lower in target_chosung:
            return True
    
    # 혼합 검색 (초성 + 일반 문자)
    target_chosung = get_chosung_string(target_lower)
    if query_lower in target_chosung:
        return True
    
    return False

@st.cache_data(ttl=300)
def load_all_product_names():
    """uploaded_products 테이블에서 제품명 목록 로드 (캐시 5분)"""
    all_names = set()
    page_size = 1000
    offset = 0

    while True:
        result = supabase.table("uploaded_products").select("product_name").order("product_name").range(offset, offset + page_size - 1).execute()
        if not result.data:
            break
        for row in result.data:
            name = str(row.get("product_name", "")).strip()
            if name:
                all_names.add(name)
        if len(result.data) < page_size:
            break
        offset += page_size
    
    return sorted(all_names)

# ========================
# 판매 데이터 DB 조회
# ========================

@st.cache_data(ttl=300)
def load_sales_for_week(monday):
    """월~토 6일간의 판매 데이터 조회 (페이지네이션, 캐시 5분)"""
    saturday = monday + timedelta(days=5)
    all_data = []
    page_size = 1000
    offset = 0
    
    while True:
        result = supabase.table("sales").select("*").gte(
            "sale_date", monday.strftime('%Y-%m-%d')
        ).lte(
            "sale_date", saturday.strftime('%Y-%m-%d')
        ).order("sale_date").order("product_name").range(offset, offset + page_size - 1).execute()
        
        if not result.data:
            break
        all_data.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size
    
    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame(columns=["id", "sale_date", "product_code", "product_name", "quantity"])


@st.cache_data(ttl=300)
def load_sales_last_month(base_date):
    """기준일로부터 최근 30일간 판매 데이터 조회 (캐시 5분)"""
    end_date = base_date
    start_date = base_date - timedelta(days=30)
    all_data = []
    page_size = 1000
    offset = 0

    while True:
        result = supabase.table("sales").select("*").gte(
            "sale_date", start_date.strftime('%Y-%m-%d')
        ).lte(
            "sale_date", end_date.strftime('%Y-%m-%d')
        ).order("sale_date").order("product_name").range(offset, offset + page_size - 1).execute()

        if not result.data:
            break
        all_data.extend(result.data)
        if len(result.data) < page_size:
            break
        offset += page_size

    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame(columns=["id", "sale_date", "product_code", "product_name", "quantity"])


def calc_avg_sales_by_dow(sales_df):
    """판매 데이터에서 제품코드별, 요일별 가중 평균 판매량 계산
    가중치: (최근 7일 평균 × 0.5) + (최근 14일 평균 × 0.3) + (최근 30일 평균 × 0.2)
    반환: { product_code: {0: avg_mon, 1: avg_tue, ..., 6: avg_sun} }
    """
    if sales_df.empty:
        return {}

    df = sales_df.copy()
    df["sale_date_dt"] = pd.to_datetime(df["sale_date"], errors="coerce")
    df["dow"] = df["sale_date_dt"].dt.weekday  # 0=월, 1=화, ..., 6=일
    df["product_code"] = df["product_code"].astype(str).str.strip()
    df["quantity"] = df["quantity"].fillna(0).astype(int)

    max_date = df["sale_date_dt"].max()

    # 기간별 데이터 분리
    df_7 = df[df["sale_date_dt"] > max_date - timedelta(days=7)]
    df_14 = df[df["sale_date_dt"] > max_date - timedelta(days=14)]
    df_30 = df  # 전체 (최근 30일)

    def _calc_dow_avg(sub_df):
        """서브 데이터프레임에서 제품코드×요일별 평균 계산 (쉬는날 제외)
        반환: { product_code: {dow: avg, ...}, ... }  — 데이터 없는 요일은 키 자체가 없음
        """
        if sub_df.empty:
            return {}
        # 쉬는날 제외: 해당 날짜의 전체 판매량이 0인 날은 평균 계산에서 제외
        daily_total = sub_df.groupby("sale_date_dt")["quantity"].sum()
        rest_days = set(daily_total[daily_total == 0].index)
        filtered_df = sub_df[~sub_df["sale_date_dt"].isin(rest_days)]
        if filtered_df.empty:
            return {}
        date_dow = filtered_df[["sale_date_dt", "dow"]].drop_duplicates()
        dow_count = date_dow.groupby("dow").size().to_dict()
        grouped = filtered_df.groupby(["product_code", "dow"])["quantity"].sum().reset_index()
        avg_map = {}
        for _, row in grouped.iterrows():
            code = row["product_code"]
            dow = int(row["dow"])
            total_qty = int(row["quantity"])
            weeks = dow_count.get(dow, 1)
            avg = total_qty / weeks
            if code not in avg_map:
                avg_map[code] = {}
            avg_map[code][dow] = avg
        return avg_map

    avg_7 = _calc_dow_avg(df_7)
    avg_14 = _calc_dow_avg(df_14)
    avg_30 = _calc_dow_avg(df_30)

    # 가중치 설정: (기간, 기본 가중치)
    WEIGHTS = [(avg_7, 0.5), (avg_14, 0.3), (avg_30, 0.2)]

    # 모든 제품코드 수집
    all_codes = set(list(avg_7.keys()) + list(avg_14.keys()) + list(avg_30.keys()))

    result = {}
    for code in all_codes:
        result[code] = {}
        for dow in range(7):
            # 각 기간에서 해당 요일 데이터가 있는 것만 수집
            available = []
            for avg_map, w in WEIGHTS:
                val = avg_map.get(code, {}).get(dow)
                if val is not None:
                    available.append((val, w))

            if not available:
                result[code][dow] = 0
            else:
                # 데이터 있는 기간끼리 가중치 재분배
                total_w = sum(w for _, w in available)
                weighted = sum(v * (w / total_w) for v, w in available)
                result[code][dow] = math.ceil(weighted)  # 올림

    return result


def get_products_in_sales(sales_df):
    """판매 데이터에 있는 고유 제품 목록"""
    if sales_df.empty:
        return []
    products = sales_df[["product_code", "product_name"]].drop_duplicates()
    return products.sort_values("product_name").to_dict("records")

def parse_inventory_file(uploaded_file):
    """재고 엑셀 파일 파싱 (레거시 호환용)"""
    df = pd.read_excel(uploaded_file)
    
    col_map = {}
    for col in df.columns:
        col_lower = str(col).lower().replace(" ", "")
        if "코드" in col_lower or "code" in col_lower:
            col_map[col] = "제품코드"
        elif "제품" in col_lower or "품목" in col_lower or "name" in col_lower or "이름" in col_lower:
            if "코드" not in col_lower and "code" not in col_lower:
                col_map[col] = "제품"
        elif "재고" in col_lower or "stock" in col_lower or "inventory" in col_lower:
            col_map[col] = "현 재고"
        elif "생산시간" in col_lower or "time" in col_lower or "초" in col_lower:
            if "시점" not in col_lower:
                col_map[col] = "개당 생산시간(초)"
        elif "시점" in col_lower or "timing" in col_lower:
            col_map[col] = "생산시점"
        elif "최소" in col_lower or "min" in col_lower:
            col_map[col] = "최소생산수량"
    
    df = df.rename(columns=col_map)
    
    if "제품코드" not in df.columns:
        return None, "제품코드 컬럼이 없습니다."
    if "제품" not in df.columns:
        return None, "제품(제품명) 컬럼이 없습니다."
    
    if "현 재고" not in df.columns:
        df["현 재고"] = 0
    if "개당 생산시간(초)" not in df.columns:
        df["개당 생산시간(초)"] = 0
    if "생산시점" not in df.columns:
        df["생산시점"] = "주야"
    if "최소생산수량" not in df.columns:
        df["최소생산수량"] = 0
    
    df["제품코드"] = df["제품코드"].astype(str).str.strip()
    df["제품"] = df["제품"].astype(str).str.strip()
    df["현 재고"] = df["현 재고"].fillna(0).astype(int)
    df["개당 생산시간(초)"] = df["개당 생산시간(초)"].fillna(0).astype(int)
    df["생산시점"] = df["생산시점"].fillna("주야").astype(str).str.strip()
    df["최소생산수량"] = df["최소생산수량"].fillna(0).astype(int)
    
    df = df.dropna(subset=["제품코드", "제품"])
    
    return df, None


@st.cache_data(ttl=300)
def load_inventory_from_db():
    """uploaded_products DB에서 재고 + 생산정보를 가져와 inventory_df 형태로 반환 (캐시 5분)"""
    result = supabase.table("uploaded_products").select("*").order("id").execute()
    if not result.data:
        return pd.DataFrame(columns=["제품코드", "제품", "현 재고", "개당 생산시간(초)", "생산시점", "최소생산수량"])
    
    df = pd.DataFrame(result.data)
    
    inv_df = pd.DataFrame()
    inv_df["제품코드"] = df["product_code"].astype(str).str.strip()
    inv_df["제품"] = df["product_name"].astype(str).str.strip()
    inv_df["현 재고"] = df["current_stock"].fillna(0).astype(int) if "current_stock" in df.columns else 0
    inv_df["개당 생산시간(초)"] = df["production_time_per_unit"].fillna(0).astype(int) if "production_time_per_unit" in df.columns else 0
    inv_df["생산시점"] = df["production_point"].fillna("주야").astype(str).str.strip().replace("", "주야") if "production_point" in df.columns else "주야"
    inv_df["최소생산수량"] = df["minimum_production_quantity"].fillna(0).astype(int) if "minimum_production_quantity" in df.columns else 0
    
    # 빈 생산시점은 "주야"로 기본값
    inv_df.loc[inv_df["생산시점"] == "", "생산시점"] = "주야"
    
    inv_df = inv_df.dropna(subset=["제품코드", "제품"])
    
    return inv_df

def build_weekly_data(avg_sales_map, inventory_df):
    """재고 + 요일별 평균 판매량으로 주간 데이터 생성.
    avg_sales_map: { product_code: {0: avg_mon, 1: avg_tue, ...} }
    반환: DataFrame (제품, 제품코드, 현 재고, 월~금, 다음주월, 다음주화, 생산시점, 최소생산수량)
    """
    rows = []
    unmatched = []

    for _, inv_row in inventory_df.iterrows():
        product_code = str(inv_row["제품코드"]).strip()
        product_name = str(inv_row["제품"]).strip()
        stock = int(inv_row.get("현 재고", 0))
        prod_time = int(inv_row.get("개당 생산시간(초)", 0))
        timing = str(inv_row.get("생산시점", "주야")).strip()
        min_qty = int(inv_row.get("최소생산수량", 0)) if "최소생산수량" in inv_row.index else 0

        # 최소생산수량 > 0 인 제품만 대상
        if min_qty <= 0:
            continue

        avg = avg_sales_map.get(product_code)
        if avg is None:
            unmatched.append(product_name)
            continue

        mon = avg.get(0, 0)
        tue = avg.get(1, 0)
        wed = avg.get(2, 0)
        thu = avg.get(3, 0)
        fri = avg.get(4, 0)
        sat = avg.get(5, 0)
        weekly_sum = mon + tue + wed + thu + fri + sat

        row = {
            "제품": product_name,
            "제품코드": product_code,
            "월": mon,
            "화": tue,
            "수": wed,
            "목": thu,
            "금": fri,
            "토": sat,
            "합계": weekly_sum,
            "다음주월": avg.get(0, 0),  # 다음주 월요일 = 월요일 평균
            "다음주화": avg.get(1, 0),  # 다음주 화요일 = 화요일 평균
            "현 재고": stock,
            "개당 생산시간(초)": prod_time,
            "최소생산수량": min_qty,
            "생산시점": timing,
        }
        rows.append(row)

    return pd.DataFrame(rows), unmatched


# ========================
# 스케줄 생성 함수 (새 조건)
# ========================

def create_schedule_from_weekly(weekly_df, start_date):
    """새 조건 기반 스케줄 생성
    
    조건:
    1. 현재 재고는 항상 요일별 평균 판매량 이상 유지
    2. 연속 최소 2일치 평균 판매량 합을 충족
    3. 부족 예상일 기준 최소 2일 전 생산 시작
    4. 금요일 이후 다음주 월요일 판매량까지 고려
    5. 주간/야간 각각 200개 제한
    6. 초과 시 다음날로 이월
    """
    df = weekly_df.copy()

    if "생산시점" not in df.columns:
        df["생산시점"] = "주야"
    df["생산시점"] = df["생산시점"].fillna("주야").astype(str).str.strip()
    df.loc[df["생산시점"] == "", "생산시점"] = "주야"

    if "최소생산수량" not in df.columns:
        df["최소생산수량"] = 0
    df["최소생산수량"] = df["최소생산수량"].fillna(0).astype(int)

    monday = get_week_monday(start_date)
    date_labels = {}
    for i, d in enumerate(DAYS):
        current_date = monday + timedelta(days=i)
        date_labels[d] = f"{current_date.strftime('%m/%d')} ({d})"

    # === 1단계: 제품별 부족일 탐색 및 생산 계획 수립 ===
    # 요일 인덱스: 월=0, 화=1, 수=2, 목=3, 금=4, 다음주월=5, 다음주화=6
    extended_days = DAYS + ["다음주월", "다음주화"]  # 금요일 이후 다음주 화요일까지 고려

    production_plan = []  # { product, produce_day_idx, qty, timing, reason }

    LOOKAHEAD = 3  # 오늘 포함 3일 선행 체크 (2일 전 생산)

    for _, row in df.iterrows():
        p = row["제품"]
        product_code = str(row.get("제품코드", "")).strip()
        sec = int(row.get("개당 생산시간(초)", 0))
        min_qty = int(row["최소생산수량"])
        timing = str(row["생산시점"]).strip()
        safety = SAFETY_STOCK.get(product_code, 0)  # 안전재고 기준

        # 요일별 판매량 배열 (월~금 + 다음주월 + 다음주화)
        sales = []
        for d in DAYS:
            sales.append(int(row.get(d, 0)))
        sales.append(int(row.get("다음주월", row.get("월", 0))))  # 인덱스5
        sales.append(int(row.get("다음주화", row.get("화", 0))))  # 인덱스6

        ext_day_names = DAYS + ["다음주월", "다음주화"]

        stock = int(row["현 재고"])

        # === 정방향 시뮬레이션 ===
        # 월~금(0~4)만 생산 가능, 판매는 0~6(다음주화)까지 고려
        production = [0] * 5  # 월~금 생산량
        prod_reasons = [""] * 5  # 월~금 생산 이유
        sim_stock = stock

        for prod_day in range(5):  # 월(0) ~ 금(4)
            # 오늘 생산분 재고 반영
            sim_stock += production[prod_day]

            # 3일 선행 체크: 오늘~모레까지 판매 후 재고가 안전재고 밑으로 떨어지는지
            look_stock = sim_stock
            need_produce = False
            max_shortage = 0
            shortage_days = []  # 부족이 발생하는 날 이름 수집

            look_end = min(prod_day + LOOKAHEAD, 7)
            for look in range(prod_day, look_end):
                look_stock -= sales[look]
                if look_stock < safety:
                    need_produce = True
                    max_shortage = max(max_shortage, safety - look_stock)
                    shortage_days.append(ext_day_names[look])

            # 부족 감지 → 오늘 생산 (최소생산수량 보장)
            if need_produce and production[prod_day] == 0:
                qty = max(max_shortage, min_qty)
                production[prod_day] = qty
                prod_reasons[prod_day] = "/".join(dict.fromkeys(shortage_days))
                sim_stock += qty

            # 오늘 판매 차감
            sim_stock -= sales[prod_day]

        # 생산 계획 등록
        for day_idx in range(5):
            if production[day_idx] > 0:
                qty = production[day_idx]
                # 부족분이 작아도 최소생산수량 이상 보장
                qty = max(qty, min_qty)
                shortage_info = prod_reasons[day_idx]
                reason_txt = f'{shortage_info} 재고부족' if shortage_info else f'{ext_day_names[day_idx]} 생산'
                if safety > 0:
                    reason_txt += f' (안전재고 {safety})'
                production_plan.append({
                    'product': p, 'product_code': product_code,
                    'produce_day': day_idx,
                    'qty': qty, 'sec': sec, 'timing': timing,
                    'min_qty': min_qty,
                    'reason': reason_txt
                })

    # === 2단계: 생산 계획을 주간/야간 슬롯에 배치 ===
    # 정렬: 생산일 빠른 순, 수량 많은 순
    production_plan.sort(key=lambda x: (x['produce_day'], -x['qty']))

    schedule = {d: {'주간': {}, '야간': {}} for d in DAYS}
    daily_sum = {d: {'주간': 0, '야간': 0} for d in DAYS}
    daily_time = {d: {'주간': 0, '야간': 0} for d in DAYS}

    # 특수 제약 제품: 각 날짜에 이미 배치된 EXCLUSIVE 제품코드 추적
    exclusive_placed = {d: None for d in DAYS}  # 날짜별로 배치된 EXCLUSIVE 제품코드 (1개만)

    def _place_to_shift(schedule, daily_sum, daily_time, day, shift, p, place_qty, sec, reason, p_code=''):
        """교대에 수량 배치하는 헬퍼 함수"""
        if p in schedule[day][shift]:
            schedule[day][shift][p]['qty'] += place_qty
            schedule[day][shift][p]['reason'] += f" + {reason}" if reason not in schedule[day][shift][p]['reason'] else ""
        else:
            schedule[day][shift][p] = {
                'qty': place_qty, 'sec': sec, 'reason': reason, 'urgency': 0
            }
        # 집계 제외 제품은 daily_sum에 포함하지 않음
        if p_code not in EXCLUDE_FROM_LIMIT:
            daily_sum[day][shift] += place_qty
        daily_time[day][shift] += place_qty * sec

    for plan in production_plan:
        p = plan['product']
        p_code = plan.get('product_code', '')
        qty = plan['qty']
        sec = plan.get('sec', 0)
        timing = plan['timing']
        reason = plan['reason']
        target_day = plan['produce_day']
        min_qty = plan.get('min_qty', 0)
        allowed_shifts = get_allowed_shifts(timing)
        is_exclusive = p_code in EXCLUSIVE_PRODUCTS
        is_unlimited = p_code in EXCLUDE_FROM_LIMIT  # 생산량 집계 제외

        remaining = qty

        # target_day부터 금요일까지 배치 시도
        for day_idx in range(target_day, len(DAYS)):
            if remaining <= 0:
                break
            day = DAYS[day_idx]

            # ── 집계 제외 제품: 상한 무시, 즉시 전량 배치 ──
            if is_unlimited:
                current_shifts = list(allowed_shifts)
                if is_exclusive and day == "월":
                    current_shifts = ['야간']
                # 균등 분배 또는 한쪽에 전량 배치
                if len(current_shifts) == 2:
                    half1 = math.ceil(remaining / 2)
                    half2 = remaining - half1
                    for shift, alloc in zip(current_shifts, [half1, half2]):
                        if alloc > 0:
                            _place_to_shift(schedule, daily_sum, daily_time, day, shift, p, alloc, sec, reason, p_code)
                else:
                    _place_to_shift(schedule, daily_sum, daily_time, day, current_shifts[0], p, remaining, sec, reason, p_code)
                remaining = 0
                break

            # ── 특수 제약 체크: EXCLUSIVE 제품은 하루에 1품목만 ──
            if is_exclusive:
                if exclusive_placed[day] is not None and exclusive_placed[day] != p_code:
                    # 이 날에 이미 다른 EXCLUSIVE 제품이 배치됨 → 다음 날로
                    continue

            # ── 특수 제약: EXCLUSIVE 제품은 월요일에 야간만 가능 ──
            if is_exclusive and day == "월":
                current_shifts = ['야간']
            else:
                current_shifts = list(allowed_shifts)

            # 주야 균등 분배: 주간/야간 둘 다 가능하면 반씩 나눠 배치
            # 단, 각 교대별 배치량은 최소생산수량 이상이어야 함
            if len(current_shifts) == 2:
                avail_day = get_shift_limit(day, '주간') - daily_sum[day]['주간']
                avail_night = get_shift_limit(day, '야간') - daily_sum[day]['야간']

                # 양쪽 교대 모두 min_qty 이상 배치 가능한지 확인
                can_split = (remaining >= min_qty * 2
                             and avail_day >= min_qty
                             and avail_night >= min_qty)

                if can_split:
                    half1 = math.ceil(remaining / 2)
                    half1 = max(half1, min_qty)
                    half2 = remaining - half1
                    half2 = max(half2, min_qty)
                    # 총합 조정 시에도 양쪽 모두 min_qty 이상 유지
                    if half1 + half2 > remaining:
                        half2 = remaining - half1
                        if half2 < min_qty:
                            half1 = remaining - min_qty
                            half2 = min_qty
                    shift_alloc = {'주간': min(half1, avail_day), '야간': min(half2, avail_night)}
                else:
                    # 최소생산수량 보장을 위해 한쪽 교대에 몰아서 배치
                    if avail_day >= min_qty and avail_day >= avail_night:
                        shift_alloc = {'주간': remaining, '야간': 0}
                    elif avail_night >= min_qty:
                        shift_alloc = {'주간': 0, '야간': remaining}
                    elif avail_day >= min_qty:
                        shift_alloc = {'주간': remaining, '야간': 0}
                    else:
                        # 양쪽 모두 min_qty 미만 여유 → 다음 날로 이월되도록 skip
                        shift_alloc = {'주간': 0, '야간': 0}

                for shift in current_shifts:
                    if remaining <= 0:
                        break
                    target_qty = shift_alloc[shift]
                    if target_qty <= 0:
                        continue
                    available = get_shift_limit(day, shift) - daily_sum[day][shift]
                    if available <= 0:
                        continue

                    place_qty = min(target_qty, available)
                    # 배치량이 min_qty 미만이면 이 교대에 넣지 않음 (다른 교대나 다음 날로)
                    if place_qty < min_qty and remaining > place_qty:
                        continue
                    _place_to_shift(schedule, daily_sum, daily_time, day, shift, p, place_qty, sec, reason, p_code)
                    remaining -= place_qty
                    if is_exclusive:
                        exclusive_placed[day] = p_code

                # 한쪽이 용량 초과로 못 넣은 잔량을 다른쪽에 추가 배치
                for shift in current_shifts:
                    if remaining <= 0:
                        break
                    available = get_shift_limit(day, shift) - daily_sum[day][shift]
                    if available <= 0:
                        continue

                    place_qty = min(remaining, available)
                    # 잔량 배치 시에도 min_qty 보장 (단, 잔량 전부를 넣는 경우는 허용)
                    if place_qty < min_qty and remaining > place_qty:
                        continue
                    _place_to_shift(schedule, daily_sum, daily_time, day, shift, p, place_qty, sec, reason, p_code)
                    remaining -= place_qty
                    if is_exclusive:
                        exclusive_placed[day] = p_code
            else:
                # 주간만 또는 야간만 가능한 경우
                for shift in current_shifts:
                    if remaining <= 0:
                        break
                    available = get_shift_limit(day, shift) - daily_sum[day][shift]
                    if available <= 0:
                        continue

                    place_qty = min(remaining, available)
                    # 배치량이 min_qty 미만이면 다음 날로 이월 (단, 잔량 전부 넣는 경우는 허용)
                    if place_qty < min_qty and remaining > place_qty:
                        continue
                    _place_to_shift(schedule, daily_sum, daily_time, day, shift, p, place_qty, sec, reason, p_code)
                    remaining -= place_qty
                    if is_exclusive:
                        exclusive_placed[day] = p_code

    return schedule, daily_sum, daily_time, date_labels, monday

# ========================
# Supabase DB 함수
# ========================

def _clear_schedule_db_caches():
    """스케줄 DB 관련 캐시 일괄 클리어"""
    load_schedule_from_db.clear()
    get_all_weeks.clear()

def delete_schedule(week_start):
    client = get_supabase_client()
    client.table("schedules").delete().eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).execute()
    _clear_schedule_db_caches()

def check_schedule_exists(week_start):
    result = supabase.table("schedules").select("id", count="exact").eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).execute()
    return result.count > 0

def save_schedule_to_db(schedule, date_labels, monday):
    client = get_supabase_client()
    friday = monday + timedelta(days=4)
    rows = []
    for day in DAYS:
        for shift in ['주간', '야간']:
            for product, data in schedule[day][shift].items():
                rows.append({
                    "week_start": monday.strftime('%Y-%m-%d'),
                    "week_end": friday.strftime('%Y-%m-%d'),
                    "day_of_week": date_labels[day],
                    "shift": shift,
                    "product": product,
                    "quantity": data['qty'],
                    "production_time": round(data['qty'] * data['sec'] / 3600, 1),
                    "reason": data['reason'],
                    "urgency": data['urgency']
                })
    if rows:
        client.table("schedules").insert(rows).execute()
    _clear_schedule_db_caches()

@st.cache_data(ttl=300)
def load_schedule_from_db(week_start_str):
    """스케줄 데이터 로드 (캐시 5분). week_start_str: 'YYYY-MM-DD' 문자열"""
    result = supabase.table("schedules").select("*").eq(
        "week_start", week_start_str
    ).order("id").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame()

def delete_schedule_row(row_id):
    """단일 행 삭제"""
    client = get_supabase_client()
    client.table("schedules").delete().eq("id", row_id).execute()
    _clear_schedule_db_caches()

def update_schedule_row(row_id, day_of_week=None, shift=None, quantity=None, production_time=None):
    """단일 행 수정 (이동 또는 수량 변경)"""
    client = get_supabase_client()
    updates = {}
    if day_of_week is not None:
        updates["day_of_week"] = day_of_week
    if shift is not None:
        updates["shift"] = shift
    if quantity is not None:
        updates["quantity"] = quantity
    if production_time is not None:
        updates["production_time"] = production_time
    if updates:
        client.table("schedules").update(updates).eq("id", row_id).execute()
        _clear_schedule_db_caches()

def backup_schedule_to_session(week_start):
    """수정 모드 진입 시 현재 스케줄을 session_state에 백업"""
    result = supabase.table("schedules").select("*").eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).order("id").execute()
    if result.data:
        st.session_state['schedule_backup'] = result.data
    else:
        st.session_state['schedule_backup'] = []

def restore_schedule_from_session(week_start):
    """취소 시 백업 데이터로 DB 복원"""
    client = get_supabase_client()
    backup = st.session_state.get('schedule_backup', [])
    if not backup:
        return

    # 현재 데이터 전체 삭제
    client.table("schedules").delete().eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).execute()

    # 백업 데이터 재삽입 (id 제외 - DB에서 자동 생성)
    rows_to_insert = []
    for row in backup:
        new_row = {k: v for k, v in row.items() if k != 'id'}
        rows_to_insert.append(new_row)

    if rows_to_insert:
        # 배치 삽입 (1000건씩)
        for i in range(0, len(rows_to_insert), 1000):
            batch = rows_to_insert[i:i+1000]
            client.table("schedules").insert(batch).execute()

    _clear_schedule_db_caches()
    st.session_state['schedule_backup'] = []

@st.cache_data(ttl=300)
def get_all_weeks():
    """주차 목록 조회 (캐시 5분)"""
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
    return []

# ========================
# 스케줄 스크린샷 생성 (Pillow)
# ========================

def get_korean_font_path():
    """시스템에서 한글 폰트 경로 찾기, 없으면 자동 다운로드"""
    candidates = [
        # 프로젝트 내 폰트 (최우선)
        os.path.join(os.path.dirname(__file__), "fonts", "NanumGothic.ttf"),
        os.path.join(os.path.dirname(__file__), "NanumGothic.ttf"),
        # Windows
        "C:/Windows/Fonts/malgun.ttf",
        "C:/Windows/Fonts/malgunbd.ttf",
        "C:/Windows/Fonts/NanumGothic.ttf",
        "C:/Windows/Fonts/gulim.ttc",
        # Linux (apt: fonts-nanum)
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        "/usr/share/fonts/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/unfonts-core/UnDotum.ttf",
        # macOS
        "/System/Library/Fonts/AppleSDGothicNeo.ttc",
        "/Library/Fonts/NanumGothic.ttf",
    ]
    for fp in candidates:
        if os.path.exists(fp):
            return fp
    
    # 시스템에 한글 폰트가 없으면 자동 다운로드
    try:
        import urllib.request
        font_dir = os.path.join(os.path.dirname(__file__), "fonts")
        os.makedirs(font_dir, exist_ok=True)
        font_path = os.path.join(font_dir, "NanumGothic.ttf")
        if not os.path.exists(font_path):
            url = "https://github.com/googlefonts/nanum/raw/main/fonts/NanumGothic-Regular.ttf"
            urllib.request.urlretrieve(url, font_path)
        return font_path
    except Exception:
        return None

def get_korean_font_path_bold():
    """한글 Bold 폰트 경로 찾기"""
    candidates = [
        os.path.join(os.path.dirname(__file__), "fonts", "NanumGothicBold.ttf"),
        "C:/Windows/Fonts/malgunbd.ttf",
        "C:/Windows/Fonts/NanumGothicBold.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf",
        "/usr/share/fonts/nanum/NanumGothicBold.ttf",
    ]
    for fp in candidates:
        if os.path.exists(fp):
            return fp
    return None

def make_font(size, bold=False):
    """폰트 객체 생성"""
    if bold:
        bold_path = get_korean_font_path_bold()
        if bold_path:
            try:
                return ImageFont.truetype(bold_path, size)
            except Exception:
                pass
    font_path = get_korean_font_path()
    if font_path:
        try:
            return ImageFont.truetype(font_path, size)
        except Exception:
            pass
    try:
        return ImageFont.truetype("arial.ttf", size)
    except Exception:
        return ImageFont.load_default()

def generate_schedule_image(df, selected_week, paper_size="A4"):
    """스케줄 데이터를 깔끔한 PNG 이미지로 생성 (Pillow)

    paper_size: "A3" (300 DPI, ~3500px) 또는 "A4" (300 DPI, ~2480px)
    """
    # 용지별 스케일 팩터 (인쇄 시 300 DPI 확보)
    SCALE = {"A3": 3, "A4": 2}.get(paper_size, 2)

    # 폰트 (스케일 적용)
    font_title = make_font(int(28 * SCALE), bold=True)
    font_week = make_font(int(18 * SCALE), bold=True)
    font_summary = make_font(int(16 * SCALE))
    font_day_header = make_font(int(20 * SCALE), bold=True)
    font_shift = make_font(int(16 * SCALE), bold=True)
    font_item = make_font(int(18 * SCALE))
    font_empty = make_font(int(14 * SCALE))

    # 색상
    BG = "#FFFFFF"
    HEADER_BG = "#2C3E50"
    HEADER_TEXT = "#FFFFFF"
    DAY_BG = "#FFF9E6"
    DAY_BORDER = "#E8D5A0"
    NIGHT_BG = "#EEF0F8"
    NIGHT_BORDER = "#B0B8D0"
    SUMMARY_BG = "#E8F4FD"
    SUMMARY_BORDER = "#B0D4E8"
    TEXT_COLOR = "#333333"
    MUTED = "#999999"
    DIVIDER = "#DDDDDD"

    # 레이아웃 상수 (스케일 적용)
    IMG_W = 1100 * SCALE
    PAD_X = 40 * SCALE
    CONTENT_W = IMG_W - PAD_X * 2
    COL_W = CONTENT_W // 2 - 10 * SCALE
    ITEM_H = 28 * SCALE
    DAY_HEADER_H = 44 * SCALE
    SHIFT_HEADER_H = 32 * SCALE
    BLOCK_PAD = 16 * SCALE
    
    # 요일별 데이터 정리
    day_data_map = {}
    for day in DAYS:
        day_matches = df[df['day_of_week'].str.contains(day)]
        day_label = day_matches['day_of_week'].iloc[0] if len(day_matches) > 0 else f"({day})"
        
        day_items = []
        for _, r in day_matches[day_matches['shift'] == '주간'].iterrows():
            day_items.append(f"{r['product']}  {r['quantity']}개  ({r['production_time']}h)")
        
        night_items = []
        for _, r in day_matches[day_matches['shift'] == '야간'].iterrows():
            night_items.append(f"{r['product']}  {r['quantity']}개  ({r['production_time']}h)")
        
        day_data_map[day] = {'label': day_label, 'day': day_items, 'night': night_items}
    
    # 전체 높이 계산 (스케일 적용)
    total_h = (60 + 30 + 50 + 20) * SCALE  # title + week + summary + gap
    for day in DAYS:
        d = day_data_map[day]
        rows = max(len(d['day']), len(d['night']), 1)
        total_h += DAY_HEADER_H + SHIFT_HEADER_H + rows * ITEM_H + BLOCK_PAD * 2 + 12 * SCALE
    total_h += 30 * SCALE  # bottom padding
    
    # 이미지 생성
    img = Image.new("RGB", (IMG_W, total_h), BG)
    draw = ImageDraw.Draw(img)
    y = 30 * SCALE
    
    # 타이틀
    title_text = "생산 스케줄"
    bbox = draw.textbbox((0, 0), title_text, font=font_title)
    tw = bbox[2] - bbox[0]
    draw.text(((IMG_W - tw) // 2, y), title_text, fill=TEXT_COLOR, font=font_title)
    y += 42 * SCALE

    # 주차 정보
    bbox = draw.textbbox((0, 0), selected_week, font=font_week)
    tw = bbox[2] - bbox[0]
    draw.text(((IMG_W - tw) // 2, y), selected_week, fill="#555555", font=font_week)
    y += 32 * SCALE
    
    # 요약
    total_qty = df['quantity'].sum()
    total_time = df['production_time'].sum()
    total_products = df['product'].nunique()
    summary = f"총 생산량: {total_qty:,}개   |   총 시간: {total_time:.1f}h   |   제품: {total_products}종"
    bbox = draw.textbbox((0, 0), summary, font=font_summary)
    sw = bbox[2] - bbox[0]
    sh = bbox[3] - bbox[1]
    sx = (IMG_W - sw) // 2 - 16 * SCALE
    draw.rounded_rectangle([sx, y - 6 * SCALE, sx + sw + 32 * SCALE, y + sh + 12 * SCALE], radius=8 * SCALE, fill=SUMMARY_BG, outline=SUMMARY_BORDER)
    draw.text(((IMG_W - sw) // 2, y), summary, fill=TEXT_COLOR, font=font_summary)
    y += sh + 30 * SCALE

    # 구분선
    draw.line([(PAD_X, y), (IMG_W - PAD_X, y)], fill=DIVIDER, width=SCALE)
    y += 16 * SCALE
    
    # 각 요일
    for day in DAYS:
        data = day_data_map[day]
        num_rows = max(len(data['day']), len(data['night']), 1)

        # 요일 헤더
        draw.rounded_rectangle(
            [PAD_X, y, IMG_W - PAD_X, y + DAY_HEADER_H],
            radius=6 * SCALE, fill=HEADER_BG
        )
        label_text = f"  {data['label']}"
        bbox = draw.textbbox((0, 0), label_text, font=font_day_header)
        lw = bbox[2] - bbox[0]
        draw.text(((IMG_W - lw) // 2, y + 10 * SCALE), label_text, fill=HEADER_TEXT, font=font_day_header)
        y += DAY_HEADER_H + 6 * SCALE

        block_h = SHIFT_HEADER_H + num_rows * ITEM_H + BLOCK_PAD

        # 주간 배경
        left_x = PAD_X
        draw.rounded_rectangle(
            [left_x, y, left_x + COL_W, y + block_h],
            radius=6 * SCALE, fill=DAY_BG, outline=DAY_BORDER
        )
        draw.text((left_x + 12 * SCALE, y + 6 * SCALE), "[주간]", fill="#B8860B", font=font_shift)

        # 야간 배경
        right_x = PAD_X + COL_W + 20 * SCALE
        draw.rounded_rectangle(
            [right_x, y, right_x + COL_W, y + block_h],
            radius=6 * SCALE, fill=NIGHT_BG, outline=NIGHT_BORDER
        )
        draw.text((right_x + 12 * SCALE, y + 6 * SCALE), "[야간]", fill="#4A5080", font=font_shift)

        item_y = y + SHIFT_HEADER_H + 4 * SCALE

        # 주간 항목
        if data['day']:
            for i, item in enumerate(data['day']):
                draw.text((left_x + 16 * SCALE, item_y + i * ITEM_H), f"• {item}", fill=TEXT_COLOR, font=font_item)
        else:
            draw.text((left_x + COL_W // 2 - 30 * SCALE, item_y + (num_rows * ITEM_H) // 2 - 10 * SCALE), "생산 없음", fill=MUTED, font=font_empty)

        # 야간 항목
        if data['night']:
            for i, item in enumerate(data['night']):
                draw.text((right_x + 16 * SCALE, item_y + i * ITEM_H), f"• {item}", fill=TEXT_COLOR, font=font_item)
        else:
            draw.text((right_x + COL_W // 2 - 30 * SCALE, item_y + (num_rows * ITEM_H) // 2 - 10 * SCALE), "생산 없음", fill=MUTED, font=font_empty)

        y += block_h + 12 * SCALE
    
    # PNG로 저장 (300 DPI 메타데이터 포함)
    buf = BytesIO()
    img.save(buf, format="PNG", dpi=(300, 300))
    buf.seek(0)
    return buf

# ========================
# 메인 앱
# ========================

st.title("📅 스케줄 관리")

# ── 데이터 새로고침 버튼 (제품/재고 변경 후 즉시 반영)
_col_menu, _col_refresh = st.columns([6, 1])
with _col_menu:
    _schedule_menu_options = ["🔍 스케줄 조회", "📈 통계"]
    if can_edit("schedule"):
        _schedule_menu_options = ["📅 새 스케줄 생성", "✏️ 직접 생성", "🔍 스케줄 조회", "📈 통계"]
    menu = st.radio("선택", _schedule_menu_options, horizontal=True)
with _col_refresh:
    st.markdown("<div style='height: 0.5rem'></div>", unsafe_allow_html=True)
    if st.button("🔄 새로고침", key="schedule_refresh", help="제품/재고 변경사항을 즉시 반영합니다"):
        load_inventory_from_db.clear()
        load_all_product_names.clear()
        load_sales_last_month.clear()
        load_sales_for_week.clear()
        _clear_schedule_db_caches()
        st.toast("✅ 데이터를 새로고침했습니다.")
        st.rerun()

st.divider()

if menu == "📅 새 스케줄 생성":
    st.header("새 생산 스케줄 생성")

    # ── Step 1: 스케줄 날짜 선택
    st.subheader("① 스케줄 날짜 선택")
    schedule_date = st.date_input("스케줄에 표시할 주간 (해당 주의 아무 날이나 선택)", datetime.now(), key="schedule_date")
    schedule_monday = get_week_monday(schedule_date)
    schedule_friday = schedule_monday + timedelta(days=4)

    st.info(f"📅 스케줄 날짜: **{schedule_monday.strftime('%Y-%m-%d')} (월) ~ {schedule_friday.strftime('%Y-%m-%d')} (금)**")

    # ── Step 2: 최근 30일 판매 데이터 로드 & 요일별 가중 평균 계산
    st.subheader("② 판매 데이터 (가중 평균: 7일×0.5 + 14일×0.3 + 30일×0.2)")
    base_date = schedule_monday - timedelta(days=1)  # 스케줄 시작 전날 기준
    sales_start = base_date - timedelta(days=30)
    sales_end = base_date
    sales_df = load_sales_last_month(base_date)

    if sales_df.empty:
        st.info(f"📊 조회 기간: **{sales_start.strftime('%Y-%m-%d')}** ~ **{sales_end.strftime('%Y-%m-%d')}** (30일간)")
        st.warning(f"⚠️ 해당 기간 판매 데이터가 없습니다.")
        st.caption("먼저 '판매 데이터 관리' 페이지에서 데이터를 업로드해주세요.")
    else:
        actual_start = pd.to_datetime(sales_df["sale_date"]).min().strftime('%Y-%m-%d')
        actual_end = pd.to_datetime(sales_df["sale_date"]).max().strftime('%Y-%m-%d')
        st.info(f"📊 조회 기간: **{actual_start}** ~ **{actual_end}**")
        avg_sales_map = calc_avg_sales_by_dow(sales_df)
        product_list = get_products_in_sales(sales_df)
        st.success(f"✅ 판매 데이터 {len(sales_df):,}건 조회 → 요일별 가중 평균 계산 완료 (제품 {len(avg_sales_map)}종)")

    if not sales_df.empty:
        # ── Step 3: 재고/생산정보 불러오기 (DB 기반)
        st.subheader("③ 재고/생산정보 확인")
        st.caption("📦 재고 → 제품관리 > 재고 탭  |  생산시점·최소생산수량 → 제품관리 > 제품 탭")
        st.caption("💡 **최소생산수량 > 0** 인 제품만 스케줄 대상입니다.")

        inventory_df = load_inventory_from_db()

        if inventory_df.empty:
            st.warning("⚠️ 등록된 제품이 없습니다. '제품 관리' 페이지에서 제품을 먼저 등록해주세요.")
        else:
            # 최소생산수량 > 0 인 제품만 필터
            target_inv = inventory_df[inventory_df["최소생산수량"] > 0].copy()
            st.success(f"✅ 전체 {len(inventory_df)}개 중 생산 대상 {len(target_inv)}개 (최소생산수량 > 0)")

            # 미리보기
            with st.expander("📋 생산 대상 제품 미리보기"):
                st.dataframe(
                    target_inv[["제품코드", "제품", "현 재고", "생산시점", "최소생산수량"]],
                    use_container_width=True, hide_index=True
                )

            if not target_inv.empty:
                # ── Step 4: 주간 데이터 확인 & 스케줄 생성
                st.subheader("④ 주간 데이터 확인 & 스케줄 생성")

                weekly_df, unmatched = build_weekly_data(avg_sales_map, target_inv)

                if unmatched:
                    st.warning(f"⚠️ 판매 데이터에 매칭되지 않는 제품 {len(unmatched)}개: {', '.join(unmatched[:10])}{'...' if len(unmatched) > 10 else ''}")

                if not weekly_df.empty:
                    preview_cols = ["제품", "제품코드", "합계", "현 재고", "월", "화", "수", "목", "금", "토", "다음주월", "다음주화", "생산시점", "최소생산수량"]
                    available_cols = [c for c in preview_cols if c in weekly_df.columns]
                    st.dataframe(
                        weekly_df[available_cols],
                        use_container_width=True,
                        hide_index=True
                    )
                    st.caption(f"매칭된 제품: {len(weekly_df)}개 | 기본 {DAILY_LIMIT}개 제한 (월 주간: {SHIFT_LIMITS['월']['주간']}개)")

                    st.divider()

                    exists = check_schedule_exists(schedule_monday)

                    if exists:
                        st.warning(f"⚠️ **{schedule_monday.strftime('%Y-%m-%d')} ~ {schedule_friday.strftime('%Y-%m-%d')}** 주차 스케줄이 이미 존재합니다!")
                        col_a, col_b, col_c = st.columns([1, 1, 3])
                        with col_a:
                            if st.button("🗑️ 삭제 후 새로 생성", type="primary"):
                                st.session_state['confirm_delete'] = True
                        with col_b:
                            if st.button("❌ 취소"):
                                st.session_state['confirm_delete'] = False
                                st.info("취소되었습니다.")
                    else:
                        st.session_state['confirm_delete'] = True

                    if st.session_state.get('confirm_delete', False):
                        if st.button("🚀 스케줄 생성", type="primary", key="create_schedule"):
                            with st.spinner("스케줄 생성 중..."):
                                try:
                                    if check_schedule_exists(schedule_monday):
                                        delete_schedule(schedule_monday)
                                        st.success("✅ 기존 스케줄 삭제 완료")

                                    schedule, daily_sum, daily_time, date_labels, schedule_monday = create_schedule_from_weekly(weekly_df, schedule_date)
                                    save_schedule_to_db(schedule, date_labels, schedule_monday)

                                    st.success(f"✅ 스케줄 생성 완료! ({date_labels['월']} ~ {date_labels['금']})")
                                    st.session_state['confirm_delete'] = False

                                    for day in DAYS:
                                        st.subheader(f"▶ {date_labels[day]}")
                                        col1, col2 = st.columns(2)

                                        with col1:
                                            st.markdown("**🌞 주간**")
                                            if schedule[day]['주간']:
                                                data = []
                                                for i, (p, info) in enumerate(schedule[day]['주간'].items(), 1):
                                                    sec_val = info.get('sec', 0)
                                                    time_h = round(info['qty'] * sec_val / 3600, 1) if sec_val > 0 else 0
                                                    data.append({
                                                        '순서': i, '제품': p,
                                                        '수량': f"{info['qty']}개",
                                                        '시간': f"{time_h}h" if time_h > 0 else "-",
                                                        '이유': info['reason']
                                                    })
                                                st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
                                                total_time_h = round(daily_time[day]['주간'] / 3600, 1)
                                                dl = get_shift_limit(day, '주간')
                                                st.caption(f"생산량: {daily_sum[day]['주간']}/{dl}개 | 소요시간: {total_time_h}h")
                                            else:
                                                st.info("생산 없음")

                                        with col2:
                                            st.markdown("**🌙 야간**")
                                            if schedule[day]['야간']:
                                                data = []
                                                for i, (p, info) in enumerate(schedule[day]['야간'].items(), 1):
                                                    sec_val = info.get('sec', 0)
                                                    time_h = round(info['qty'] * sec_val / 3600, 1) if sec_val > 0 else 0
                                                    data.append({
                                                        '순서': i, '제품': p,
                                                        '수량': f"{info['qty']}개",
                                                        '시간': f"{time_h}h" if time_h > 0 else "-",
                                                        '이유': info['reason']
                                                    })
                                                st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
                                                total_time_h = round(daily_time[day]['야간'] / 3600, 1)
                                                dl = get_shift_limit(day, '야간')
                                                st.caption(f"생산량: {daily_sum[day]['야간']}/{dl}개 | 소요시간: {total_time_h}h")
                                            else:
                                                st.info("생산 없음")

                                        st.divider()

                                except Exception as e:
                                    st.error(f"❌ 오류 발생: {str(e)}")
                else:
                    st.warning("매칭되는 제품이 없습니다. 제품관리에서 제품코드를 확인해주세요.")
            else:
                st.warning("최소생산수량이 설정된 제품이 없습니다. 제품 탭에서 최소생산수량을 입력해주세요.")

elif menu == "✏️ 직접 생성":
    st.header("스케줄 직접 생성")
    st.caption("제품별로 수량·교대·요일을 선택하여 스케줄을 추가합니다.")

    # ── Step 1: 주간 선택
    st.subheader("① 주간 선택")
    manual_date = st.date_input("날짜를 선택하면 해당 주의 월~금이 자동 선택됩니다", datetime.now(), key="manual_schedule_date")
    manual_monday = get_week_monday(manual_date)
    manual_friday = manual_monday + timedelta(days=4)

    week_dates = {}
    day_labels = {}
    for i, d in enumerate(DAYS):
        dt = manual_monday + timedelta(days=i)
        week_dates[d] = dt
        day_labels[d] = f"{dt.strftime('%m/%d')}({d})"

    st.info(f"📅 선택된 주간: **{manual_monday.strftime('%Y-%m-%d')} (월) ~ {manual_friday.strftime('%Y-%m-%d')} (금)**")

    # ── Step 2: 제품 목록 + 제품별 수량/교대/요일 체크
    st.subheader("② 제품별 생산 설정")

    inventory_df = load_inventory_from_db()

    if inventory_df.empty:
        st.warning("⚠️ 등록된 제품이 없습니다. '제품 관리' 페이지에서 제품을 먼저 등록해주세요.")
    else:
        search_query = st.text_input("🔍 제품 검색 (초성 검색 가능)", key="manual_product_search", placeholder="예: ㅊㄱ, 치킨, F0000")

        filtered_inv = inventory_df.copy()
        if search_query:
            mask = filtered_inv.apply(
                lambda row: match_chosung(search_query, str(row["제품"])) or search_query.upper() in str(row["제품코드"]).upper(),
                axis=1
            )
            filtered_inv = filtered_inv[mask]

        if filtered_inv.empty:
            st.info("검색 결과가 없습니다.")
        else:
            # 헤더 행
            hdr_cols = st.columns([3, 1.5, 1.5, 1, 1, 1, 1, 1])
            hdr_cols[0].markdown("**제품**")
            hdr_cols[1].markdown("**수량**")
            hdr_cols[2].markdown("**교대**")
            for i, d in enumerate(DAYS):
                hdr_cols[3 + i].markdown(f"**{day_labels[d]}**")

            st.markdown("---")

            manual_rows = []  # 수집된 입력 데이터

            for _, inv_row in filtered_inv.iterrows():
                p_code = str(inv_row["제품코드"]).strip()
                p_name = str(inv_row["제품"]).strip()
                min_qty = int(inv_row.get("최소생산수량", 0))
                prod_time = int(inv_row.get("개당 생산시간(초)", 0))
                timing = str(inv_row.get("생산시점", "주야")).strip()
                if not timing:
                    timing = "주야"

                cols = st.columns([3, 1.5, 1.5, 1, 1, 1, 1, 1])

                with cols[0]:
                    st.markdown(f"<div style='padding-top:0.35rem;font-size:0.85rem'><b>{p_name}</b><br><span style='color:gray'>{p_code}</span></div>", unsafe_allow_html=True)

                with cols[1]:
                    qty = st.number_input("수량", min_value=0, value=0, step=10, key=f"mq_{p_code}", label_visibility="collapsed")

                with cols[2]:
                    shift_options = get_allowed_shifts(timing)
                    if len(shift_options) == 1:
                        shift = shift_options[0]
                        st.text_input("교대", value=shift, disabled=True, key=f"ms_d_{p_code}", label_visibility="collapsed")
                    else:
                        shift = st.selectbox("교대", options=shift_options, key=f"ms_{p_code}", label_visibility="collapsed")

                checked_days = []
                for i, d in enumerate(DAYS):
                    with cols[3 + i]:
                        if st.checkbox(d, value=False, key=f"md_{p_code}_{d}", label_visibility="collapsed"):
                            checked_days.append(d)

                if qty > 0 and checked_days:
                    manual_rows.append({
                        "product": p_name,
                        "product_code": p_code,
                        "quantity": qty,
                        "shift": shift,
                        "production_time": round(qty * prod_time / 3600, 1),
                        "days": checked_days,
                    })

            st.divider()

            # ── 미리보기 (기존 + 신규)
            if manual_rows:
                st.subheader("③ 미리보기")

                existing_df = load_schedule_from_db(manual_monday.strftime('%Y-%m-%d'))

                existing_data = []
                if not existing_df.empty:
                    for _, row in existing_df.iterrows():
                        existing_data.append({
                            "구분": "📌 기존",
                            "날짜": row["day_of_week"],
                            "교대": row["shift"],
                            "제품": row["product"],
                            "수량": f"{row['quantity']}개",
                            "소요시간": f"{row['production_time']}h",
                        })

                new_data = []
                for mr in manual_rows:
                    for d in mr["days"]:
                        new_data.append({
                            "구분": "🆕 추가",
                            "날짜": day_labels[d],
                            "교대": mr["shift"],
                            "제품": mr["product"],
                            "수량": f"{mr['quantity']}개",
                            "소요시간": f"{mr['production_time']}h",
                        })

                combined = existing_data + new_data
                combined_df = pd.DataFrame(combined)

                if existing_data:
                    st.caption(f"📌 기존 스케줄 {len(existing_data)}건 + 🆕 새로 추가 {len(new_data)}건")
                else:
                    st.caption(f"🆕 새로 추가 {len(new_data)}건")
                st.dataframe(combined_df, use_container_width=True, hide_index=True)

                st.divider()

                # 저장 버튼
                if st.button("💾 스케줄 저장", type="primary", key="manual_save_schedule"):
                    with st.spinner("저장 중..."):
                        try:
                            rows_to_insert = []
                            for mr in manual_rows:
                                for d in mr["days"]:
                                    rows_to_insert.append({
                                        "week_start": manual_monday.strftime('%Y-%m-%d'),
                                        "week_end": manual_friday.strftime('%Y-%m-%d'),
                                        "day_of_week": day_labels[d],
                                        "shift": mr["shift"],
                                        "product": mr["product"],
                                        "quantity": mr["quantity"],
                                        "production_time": mr["production_time"],
                                        "reason": "직접 생성",
                                        "urgency": 0,
                                    })

                            if rows_to_insert:
                                client = get_supabase_client()
                                for i in range(0, len(rows_to_insert), 1000):
                                    batch = rows_to_insert[i:i+1000]
                                    client.table("schedules").insert(batch).execute()
                                _clear_schedule_db_caches()
                                st.success(f"✅ {len(rows_to_insert)}건 스케줄 저장 완료!")
                                st.toast("스케줄이 저장되었습니다.")
                            else:
                                st.warning("저장할 데이터가 없습니다.")
                        except Exception as e:
                            st.error(f"❌ 저장 오류: {str(e)}")
            else:
                st.info("💡 제품의 수량을 입력하고 요일을 체크해주세요.")

elif menu == "🔍 스케줄 조회":
    st.header("저장된 스케줄 조회")

    weeks = get_all_weeks()

    if not weeks:
        st.info("저장된 스케줄이 없습니다. 먼저 스케줄을 생성해주세요.")
    else:
        week_options = [f"{w[0]} ~ {w[1]}" for w in weeks]
        selected_week = st.selectbox("주차 선택", week_options)

        if selected_week:
            week_start = datetime.strptime(weeks[week_options.index(selected_week)][0], '%Y-%m-%d')
            week_start_str = week_start.strftime('%Y-%m-%d')
            df = load_schedule_from_db(week_start_str)

            if not df.empty:
                # 수정 모드 토글 (주차별로 저장, 주차 변경 시 초기화)
                is_edit_mode = can_edit("schedule") and st.session_state.get('schedule_edit_week') == selected_week and st.session_state.get('schedule_edit_mode', False)

                # ── 요일별 데이터 사전 인덱싱 (한 번만 수행)
                day_data_map = {}
                for day in DAYS:
                    day_df = df[df['day_of_week'].str.contains(day)]
                    day_label = day_df['day_of_week'].iloc[0] if len(day_df) > 0 else f"({day})"
                    day_data_map[day] = {
                        'label': day_label,
                        'df': day_df,
                        'day_shift': day_df[day_df['shift'] == '주간'] if not day_df.empty else pd.DataFrame(),
                        'night_shift': day_df[day_df['shift'] == '야간'] if not day_df.empty else pd.DataFrame(),
                    }
                day_labels_list = [day_data_map[d]['label'] for d in DAYS]

                # 상단 버튼 배치: 수정/완료/취소(왼쪽) + 다운로드(오른쪽)
                if can_edit("schedule"):
                    col_edit_btn, col_cancel_btn, col_del_btn, _, col_dl_excel, col_dl_img = st.columns([1, 1, 1, 0.5, 1, 1])
                else:
                    _, col_dl_excel, col_dl_img = st.columns([3.5, 1, 1])
                if can_edit("schedule"):
                    with col_edit_btn:
                        if not is_edit_mode:
                            if st.button("✏️ 수정", key="btn_edit_schedule"):
                                backup_schedule_to_session(week_start)
                                st.session_state['schedule_edit_mode'] = True
                                st.session_state['schedule_edit_week'] = selected_week
                                st.rerun()
                        else:
                            if st.button("✔️ 수정 완료", key="btn_done_edit"):
                                st.session_state['schedule_edit_mode'] = False
                                st.session_state['schedule_edit_week'] = None
                                st.session_state['add_product_expanded'] = False
                                st.session_state['schedule_backup'] = []
                                st.rerun()
                    with col_cancel_btn:
                        if is_edit_mode:
                            if st.button("↩️ 취소", key="btn_cancel_edit"):
                                try:
                                    restore_schedule_from_session(week_start)
                                    st.session_state['schedule_edit_mode'] = False
                                    st.session_state['schedule_edit_week'] = None
                                    st.session_state['add_product_expanded'] = False
                                    st.toast("수정 사항이 취소되었습니다.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 복원 실패: {str(e)}")
                    with col_del_btn:
                        if is_edit_mode and st.button("🗑️ 주 전체 삭제", type="secondary", key="btn_del_week_top"):
                            st.session_state['confirm_delete_schedule'] = selected_week
                            st.rerun()
                with col_dl_excel:
                    # 엑셀: 세션에 캐시하여 매 렌더 시 재생성 방지
                    excel_cache_key = f"_excel_cache_{week_start_str}"
                    if excel_cache_key not in st.session_state:
                        output = BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df.to_excel(writer, index=False, sheet_name='생산스케줄')
                        st.session_state[excel_cache_key] = output.getvalue()
                    st.download_button(
                        label="📥 엑셀 다운로드",
                        data=st.session_state[excel_cache_key],
                        file_name=f"생산스케줄_{selected_week.replace(' ~ ', '_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel"
                    )
                with col_dl_img:
                    # 용지 크기 선택 및 고해상도 이미지 다운로드
                    paper_size = st.selectbox(
                        "용지 크기", ["A4", "A3"], key="paper_size_select",
                        help="A3: 대형 인쇄용 (3300px), A4: 일반 인쇄용 (2200px)"
                    )
                    img_cache_key = f"_img_cache_{week_start_str}_{paper_size}"
                    if img_cache_key not in st.session_state:
                        try:
                            img_buf = generate_schedule_image(df, selected_week, paper_size=paper_size)
                            st.session_state[img_cache_key] = img_buf.getvalue()
                        except Exception:
                            st.session_state[img_cache_key] = None
                    if st.session_state[img_cache_key] is not None:
                        st.download_button(
                            label=f"📸 스크린샷 저장 ({paper_size})",
                            data=st.session_state[img_cache_key],
                            file_name=f"생산스케줄_{selected_week.replace(' ~ ', '_')}_{paper_size}.png",
                            mime="image/png",
                            key="download_screenshot"
                        )
                    else:
                        st.button("📸 스크린샷 저장", key="dl_screenshot_err", disabled=True)

                # 주 전체 삭제 확인
                if st.session_state.get('confirm_delete_schedule') == selected_week:
                    st.warning(f"⚠️ **{selected_week}** 스케줄을 삭제하시겠습니까? 이 작업은 되돌릴 수 없습니다.")
                    col_confirm1, col_confirm2, _ = st.columns([1, 1, 4])
                    with col_confirm1:
                        if st.button("✅ 삭제 확인", type="primary", key="confirm_del"):
                            try:
                                delete_schedule(week_start)
                                st.success("✅ 스케줄이 삭제되었습니다.")
                                st.session_state['confirm_delete_schedule'] = None
                                st.session_state['schedule_edit_mode'] = False
                                st.session_state['schedule_edit_week'] = None
                                # 다운로드 캐시 제거
                                st.session_state.pop(excel_cache_key, None)
                                st.session_state.pop(img_cache_key, None)
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ 삭제 실패: {str(e)}")
                    with col_confirm2:
                        if st.button("❌ 취소", key="cancel_del"):
                            st.session_state['confirm_delete_schedule'] = None
                            st.rerun()
                    st.divider()

                # ── 제품 추가 (수정 모드)
                if is_edit_mode:
                    with st.expander("➕ 제품 추가", expanded=False):

                        # 제품 목록 로드
                        all_product_names = load_all_product_names()

                        # 초성 검색 필터
                        search_query = st.text_input(
                            "🔍 제품 검색 (제품명 또는 초성 입력)",
                            key="add_prod_search",
                            placeholder="예: 초코파이, ㅊㅋㅍㅇ, 파이 등"
                        )

                        if search_query.strip():
                            filtered_products = [p for p in all_product_names if match_chosung(search_query.strip(), p)]
                        else:
                            filtered_products = all_product_names

                        # 직접 입력 옵션 추가
                        DIRECT_INPUT = "✏️ 직접 입력..."
                        product_options = filtered_products + [DIRECT_INPUT]

                        if not filtered_products and search_query.strip():
                            st.caption(f"'{search_query}'에 해당하는 제품이 없습니다. 직접 입력을 선택하세요.")
                            product_options = [DIRECT_INPUT]
                        elif search_query.strip():
                            st.caption(f"검색 결과: {len(filtered_products)}건")

                        selected_product = st.selectbox(
                            "제품 선택",
                            options=product_options,
                            key="add_prod_select",
                            index=0
                        )

                        # 직접 입력 선택 시
                        if selected_product == DIRECT_INPUT:
                            add_product_name = st.text_input("제품명 직접 입력", key="add_prod_name_direct", placeholder="새 제품명을 입력하세요")
                        else:
                            add_product_name = selected_product

                        add_col1, add_col2 = st.columns(2)
                        with add_col1:
                            add_quantity = st.number_input("수량 (개)", min_value=1, value=1, step=1, key="add_prod_qty")
                            add_production_time = st.number_input("생산시간 (h)", min_value=0.0, value=0.0, step=0.1, format="%.1f", key="add_prod_time")
                        with add_col2:
                            add_day = st.selectbox("요일", day_labels_list, key="add_prod_day")
                            add_shift = st.selectbox("교대", ["주간", "야간"], key="add_prod_shift")

                        add_reason = st.text_input("이유", key="add_prod_reason", placeholder="예: 긴급 추가, 수동 추가 등")

                        if st.button("✅ 제품 추가", key="btn_add_product", type="primary"):
                            final_name = add_product_name.strip() if add_product_name else ""
                            if not final_name or final_name == DIRECT_INPUT:
                                st.error("제품명을 입력 또는 선택해주세요.")
                            else:
                                try:
                                    week_end = week_start + timedelta(days=4)
                                    new_row = {
                                        "week_start": week_start_str,
                                        "week_end": week_end.strftime('%Y-%m-%d'),
                                        "day_of_week": add_day,
                                        "shift": add_shift,
                                        "product": final_name,
                                        "quantity": int(add_quantity),
                                        "production_time": round(float(add_production_time), 1),
                                        "reason": add_reason.strip() if add_reason else "수동 추가",
                                        "urgency": 0
                                    }
                                    client = get_supabase_client()
                                    client.table("schedules").insert(new_row).execute()
                                    _clear_schedule_db_caches()
                                    load_all_product_names.clear()
                                    # 다운로드 캐시 제거 (데이터 변경됨)
                                    st.session_state.pop(excel_cache_key, None)
                                    st.session_state.pop(img_cache_key, None)
                                    st.success(f"✅ **{final_name}** {int(add_quantity)}개 → {add_day} {add_shift}에 추가되었습니다.")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 추가 실패: {str(e)}")

                # ── 컬럼명 rename 사전 (보기 모드용, 한 번만 생성)
                _col_rename = {
                    'product': '제품', 'quantity': '수량(개)',
                    'production_time': '시간(h)', 'reason': '이유'
                }

                if not is_edit_mode:
                    # 보기 모드: 데이터프레임으로 표시
                    for day in DAYS:
                        dd = day_data_map[day]
                        st.subheader(f"▶ {dd['label']}")

                        if not dd['df'].empty:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**🌞 주간**")
                                if not dd['day_shift'].empty:
                                    st.dataframe(
                                        dd['day_shift'][['product', 'quantity', 'production_time', 'reason']].rename(columns=_col_rename),
                                        use_container_width=True, hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                            with col2:
                                st.markdown("**🌙 야간**")
                                if not dd['night_shift'].empty:
                                    st.dataframe(
                                        dd['night_shift'][['product', 'quantity', 'production_time', 'reason']].rename(columns=_col_rename),
                                        use_container_width=True, hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                        else:
                            st.info("생산 없음")
                        st.divider()
                else:
                    # 수정 모드: 삭제/이동/수량수정 버튼 표시
                    def _render_edit_row(row, day_labels_list):
                        """수정 모드 단일 행 렌더링 (인라인)"""
                        rid = row['id']
                        c_del, c_name, c_qty, c_day, c_shift, c_apply = st.columns([0.5, 2.5, 1.2, 1.8, 1, 0.8])
                        with c_del:
                            if st.button("🗑️", key=f"del_{rid}", help="삭제"):
                                delete_schedule_row(rid)
                                # 다운로드 캐시 제거
                                st.session_state.pop(f"_excel_cache_{week_start_str}", None)
                                st.session_state.pop(f"_img_cache_{week_start_str}", None)
                                st.rerun()
                        with c_name:
                            st.caption(f"**{row['product']}**\n{row['production_time']}h · {row.get('reason', '')}")
                        with c_qty:
                            new_qty = st.number_input("수량", min_value=1, value=int(row['quantity']), step=1, key=f"qty_{rid}", label_visibility="collapsed")
                        with c_day:
                            current_day_idx = day_labels_list.index(row['day_of_week']) if row['day_of_week'] in day_labels_list else 0
                            move_day = st.selectbox("요일", day_labels_list, index=current_day_idx, key=f"move_day_{rid}", label_visibility="collapsed")
                        with c_shift:
                            current_shift_idx = 0 if row['shift'] == '주간' else 1
                            move_shift = st.selectbox("교대", ["주간", "야간"], index=current_shift_idx, key=f"move_shift_{rid}", label_visibility="collapsed")
                        with c_apply:
                            if st.button("적용", key=f"apply_{rid}"):
                                qty_changed = int(new_qty) != int(row['quantity'])
                                moved = move_day != row['day_of_week'] or move_shift != row['shift']
                                if qty_changed or moved:
                                    updates_kw = {}
                                    if moved:
                                        updates_kw['day_of_week'] = move_day
                                        updates_kw['shift'] = move_shift
                                    if qty_changed:
                                        updates_kw['quantity'] = int(new_qty)
                                        if int(row['quantity']) > 0:
                                            time_per_unit = float(row['production_time']) / int(row['quantity'])
                                            updates_kw['production_time'] = round(int(new_qty) * time_per_unit, 1)
                                    update_schedule_row(rid, **updates_kw)
                                    # 다운로드 캐시 제거
                                    st.session_state.pop(f"_excel_cache_{week_start_str}", None)
                                    st.session_state.pop(f"_img_cache_{week_start_str}", None)
                                    st.rerun()

                    for day in DAYS:
                        dd = day_data_map[day]
                        st.subheader(f"▶ {dd['label']}")

                        if not dd['df'].empty:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**🌞 주간**")
                                if not dd['day_shift'].empty:
                                    for _, row in dd['day_shift'].iterrows():
                                        with st.container():
                                            _render_edit_row(row, day_labels_list)
                                else:
                                    st.info("생산 없음")
                            with col2:
                                st.markdown("**🌙 야간**")
                                if not dd['night_shift'].empty:
                                    for _, row in dd['night_shift'].iterrows():
                                        with st.container():
                                            _render_edit_row(row, day_labels_list)
                                else:
                                    st.info("생산 없음")
                        else:
                            st.info("생산 없음")
                        st.divider()
                

elif menu == "📈 통계":
    st.header("생산 통계")
    
    weeks = get_all_weeks()
    
    if not weeks:
        st.info("저장된 데이터가 없습니다.")
    else:
        week_options = [f"{w[0]} ~ {w[1]}" for w in weeks]
        selected_week = st.selectbox("주차 선택", week_options)
        
        if selected_week:
            week_start = datetime.strptime(weeks[week_options.index(selected_week)][0], '%Y-%m-%d')
            df = load_schedule_from_db(week_start.strftime('%Y-%m-%d'))
            
            if not df.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    daily_qty = df.groupby('day_of_week')['quantity'].sum().reset_index()
                    fig1 = px.bar(daily_qty, x='day_of_week', y='quantity',
                                 title='요일별 총 생산량',
                                 labels={'day_of_week': '요일', 'quantity': '생산량(개)'},
                                 color='quantity', color_continuous_scale='Blues')
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    shift_qty = df.groupby('shift')['quantity'].sum().reset_index()
                    fig2 = px.pie(shift_qty, values='quantity', names='shift',
                                 title='주간/야간 생산 비율',
                                 color='shift',
                                 color_discrete_map={'주간': '#1f77b4', '야간': '#ff7f0e'})
                    st.plotly_chart(fig2, use_container_width=True)
                
                product_qty = df.groupby('product')['quantity'].sum().reset_index().sort_values('quantity', ascending=False).head(10)
                fig3 = px.bar(product_qty, x='quantity', y='product', orientation='h',
                             title='제품별 생산량 TOP 10',
                             labels={'product': '제품', 'quantity': '생산량(개)'},
                             color='quantity', color_continuous_scale='Greens')
                st.plotly_chart(fig3, use_container_width=True)
                
                st.subheader("📊 주간 요약")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("총 생산량", f"{df['quantity'].sum()}개")
                with col2:
                    st.metric("총 생산시간", f"{df['production_time'].sum():.1f}시간")
                with col3:
                    st.metric("제품 종류", f"{df['product'].nunique()}개")
                with col4:
                    st.metric("평균 긴급도", f"{df['urgency'].mean():.0f}점")

