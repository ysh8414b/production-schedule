import streamlit as st
import pandas as pd
import math
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO
from supabase import create_client
from PIL import Image, ImageDraw, ImageFont
import os

# ========================
# Supabase 연결
# ========================

@st.cache_resource
def get_supabase_client():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = get_supabase_client()

# ========================
# 설정 상수
# ========================

DAYS = ["월", "화", "수", "목", "금"]
DAILY_LIMIT = 200
WORK_HOURS = 8 * 60 * 60
BATCH_SIZE = 1

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
    """products 테이블에서 제품명 목록 로드 (캐시 5분)"""
    all_names = set()
    page_size = 1000
    offset = 0
    
    while True:
        result = supabase.table("products").select("product_name").order("product_name").range(offset, offset + page_size - 1).execute()
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

def load_sales_for_week(monday):
    """월~토 6일간의 판매 데이터 조회 (페이지네이션)"""
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


def load_inventory_from_db():
    """제품관리 DB에서 재고 + 생산정보를 가져와 inventory_df 형태로 반환"""
    result = supabase.table("products").select("*").order("id").execute()
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

def build_weekly_data(sales_df, inventory_df, monday):
    """재고 파일 기준으로 주간 데이터 생성. 제품코드로 판매데이터 매칭, 제품명은 재고 파일 기준."""
    
    day_map = {}
    day_labels = ["월", "화", "수", "목", "금", "토"]
    for i, label in enumerate(day_labels):
        day_map[label] = (monday + timedelta(days=i)).strftime('%Y-%m-%d')
    
    next_monday = monday + timedelta(days=7)
    next_tuesday = monday + timedelta(days=8)
    
    rows = []
    unmatched = []
    
    for _, inv_row in inventory_df.iterrows():
        product_code = str(inv_row["제품코드"]).strip()
        product_name = str(inv_row["제품"]).strip()
        stock = int(inv_row.get("현 재고", 0))
        prod_time = int(inv_row.get("개당 생산시간(초)", 0))
        timing = str(inv_row.get("생산시점", "주야")).strip()
        min_qty = int(inv_row.get("최소생산수량", 0)) if "최소생산수량" in inv_row.index else 0
        
        # 제품코드로 판매 데이터 매칭
        prod_sales = sales_df[sales_df["product_code"].astype(str).str.strip() == product_code]
        
        if prod_sales.empty:
            unmatched.append(product_name)
            continue
        
        # 요일별 판매량 집계
        daily_qty = {}
        for label in day_labels:
            date_str = day_map[label]
            day_sales = prod_sales[prod_sales["sale_date"] == date_str]
            daily_qty[label] = int(day_sales["quantity"].sum()) if not day_sales.empty else 0
        
        # 다음주 월, 화 (없으면 이번주 값 사용)
        next_mon_sales = prod_sales[prod_sales["sale_date"] == next_monday.strftime('%Y-%m-%d')]
        next_tue_sales = prod_sales[prod_sales["sale_date"] == next_tuesday.strftime('%Y-%m-%d')]
        next_mon_qty = int(next_mon_sales["quantity"].sum()) if not next_mon_sales.empty else daily_qty["월"]
        next_tue_qty = int(next_tue_sales["quantity"].sum()) if not next_tue_sales.empty else daily_qty["화"]
        
        row = {
            "제품": product_name,
            "제품코드": product_code,
            "월": daily_qty["월"],
            "화": daily_qty["화"],
            "수": daily_qty["수"],
            "목": daily_qty["목"],
            "금": daily_qty["금"],
            "토": daily_qty["토"],
            "현 재고": stock,
            "개당 생산시간(초)": prod_time,
            "최소생산수량": min_qty,
            "생산시점": timing,
            "다음주월": next_mon_qty,
            "다음주화": next_tue_qty,
        }
        rows.append(row)
    
    return pd.DataFrame(rows), unmatched


# ========================
# 스케줄 생성 함수 (DB 기반)
# ========================

def create_schedule_from_weekly(weekly_df, start_date):
    """주간데이터 DataFrame으로부터 스케줄 생성"""
    df = weekly_df.copy()
    df["주간판매"] = df[DAYS].sum(axis=1)
    df = df[df["주간판매"] > 0].copy()
    df = df[df["현 재고"].notna()].copy()
    df["개당 생산시간(초)"] = df["개당 생산시간(초)"].fillna(0)
    if "최소생산수량" not in df.columns:
        df["최소생산수량"] = 0
    df["최소생산수량"] = df["최소생산수량"].fillna(0).astype(int)
    if "생산시점" not in df.columns:
        df["생산시점"] = "주야"
    df["생산시점"] = df["생산시점"].fillna("주야").astype(str).str.strip()
    
    monday = get_week_monday(start_date)
    date_labels = {}
    for i, d in enumerate(DAYS):
        current_date = monday + timedelta(days=i)
        date_labels[d] = f"{current_date.strftime('%m/%d')} ({d})"
    
    production_plan = []
    
    for _, row in df.iterrows():
        p = row["제품"]
        sec = int(row["개당 생산시간(초)"])
        stock = row["현 재고"]
        max_daily_sales = max([row[d] for d in DAYS])
        
        for day_idx, d in enumerate(DAYS):
            daily_sales = row[d]
            stock_after_sales = stock - daily_sales
            
            if day_idx == len(DAYS) - 1:
                sat_qty = row["토"] if "토" in row.index else 0
                next_mon = row.get("다음주월", row["월"])
                next_tue = row.get("다음주화", row["화"])
                future_sales = daily_sales + sat_qty + next_mon + next_tue
            else:
                lookahead = min(2, len(DAYS) - day_idx)
                future_sales = sum([row[DAYS[day_idx + i]] for i in range(lookahead)])
            
            if stock < future_sales or stock_after_sales < max_daily_sales:
                if stock < future_sales:
                    shortage = future_sales - stock
                    reason = "2일치 부족"
                else:
                    shortage = max_daily_sales - stock_after_sales
                    reason = "안전재고 확보"
                
                production_qty = math.ceil(shortage / BATCH_SIZE) * BATCH_SIZE
                min_qty = int(row["최소생산수량"]) if row["최소생산수량"] > 0 else 0
                if min_qty > 0:
                    production_qty = max(production_qty, min_qty)
                
                if p.startswith("(쿠)"):
                    deadline = max(0, day_idx - 2)
                    reason = reason + " (쿠:2일전)"
                else:
                    deadline = min(day_idx + 1, len(DAYS) - 1)
                
                production_plan.append({
                    'product': p,
                    'deadline': deadline,
                    'qty': production_qty,
                    'sec': sec,
                    'reason': reason,
                    'next_week': False,
                    'production_timing': str(row["생산시점"]).strip() if row["생산시점"] else "주야"
                })
                
                stock += production_qty
            
            stock -= daily_sales
    
    # 임시 배치로 최종 재고 계산
    temp_schedule = {d: {'주간': {}, '야간': {}} for d in DAYS}
    temp_daily_sum = {d: {'주간': 0, '야간': 0} for d in DAYS}
    temp_daily_time = {d: {'주간': 0, '야간': 0} for d in DAYS}
    
    for plan in production_plan:
        p = plan['product']
        deadline = plan['deadline']
        qty = plan['qty']
        sec = plan['sec']
        
        valid_days = list(range(deadline + 1))
        valid_days.sort(key=lambda x: (temp_daily_sum[DAYS[x]]['주간'] + temp_daily_sum[DAYS[x]]['야간']))
        
        placed = False
        allowed_shifts = get_allowed_shifts(plan.get('production_timing', '주야'))
        for day_idx in valid_days:
            day = DAYS[day_idx]
            for shift in allowed_shifts:
                if p in temp_schedule[day][shift]:
                    old_qty = temp_schedule[day][shift][p]['qty']
                    new_qty = old_qty + qty
                    new_time = new_qty * sec
                    if temp_daily_sum[day][shift] - old_qty + new_qty <= DAILY_LIMIT and temp_daily_time[day][shift] - (old_qty * sec) + new_time <= WORK_HOURS:
                        temp_daily_sum[day][shift] = temp_daily_sum[day][shift] - old_qty + new_qty
                        temp_daily_time[day][shift] = temp_daily_time[day][shift] - (old_qty * sec) + new_time
                        temp_schedule[day][shift][p] = {'qty': new_qty, 'sec': sec}
                        placed = True
                        break
                else:
                    if temp_daily_sum[day][shift] + qty <= DAILY_LIMIT and temp_daily_time[day][shift] + (qty * sec) <= WORK_HOURS:
                        temp_schedule[day][shift][p] = {'qty': qty, 'sec': sec}
                        temp_daily_sum[day][shift] += qty
                        temp_daily_time[day][shift] += qty * sec
                        placed = True
                        break
            if placed:
                break
    
    final_stocks = {}
    for _, row in df.iterrows():
        p = row["제품"]
        stock = row["현 재고"]
        for d in DAYS:
            for shift in ['주간', '야간']:
                if p in temp_schedule[d][shift]:
                    stock += temp_schedule[d][shift][p]['qty']
            stock -= row[d]
        final_stocks[p] = stock
    
    # 다음주 대비 추가 생산
    additional_plan = []
    for _, row in df.iterrows():
        p = row["제품"]
        sec = int(row["개당 생산시간(초)"])
        stock = final_stocks[p]
        max_daily_sales = max([row[d] for d in DAYS])
        
        for day_idx, d in enumerate(DAYS):
            daily_sales = row[d]
            stock_after_sales = stock - daily_sales
            
            if day_idx == len(DAYS) - 1:
                sat_qty = row["토"] if "토" in row.index else 0
                next_mon = row.get("다음주월", row["월"])
                future_sales = daily_sales + sat_qty + next_mon
            else:
                lookahead = min(2, len(DAYS) - day_idx)
                future_sales = sum([row[DAYS[day_idx + i]] for i in range(lookahead)])
            
            if stock < future_sales or stock_after_sales < max_daily_sales:
                if stock < future_sales:
                    shortage = future_sales - stock
                    reason = "다음주 2일치"
                else:
                    shortage = max_daily_sales - stock_after_sales
                    reason = "다음주 안전재고"
                
                production_qty = math.ceil(shortage / BATCH_SIZE) * BATCH_SIZE
                min_qty = int(row["최소생산수량"]) if row["최소생산수량"] > 0 else 0
                if min_qty > 0:
                    production_qty = max(production_qty, min_qty)
                
                if p.startswith("(쿠)"):
                    deadline = min(len(DAYS) - 3, max(0, day_idx - 2))
                    reason = reason + " (쿠:2일전)"
                else:
                    deadline = len(DAYS) - 1
                
                additional_plan.append({
                    'product': p,
                    'deadline': deadline,
                    'qty': production_qty,
                    'sec': sec,
                    'reason': reason,
                    'next_week': True,
                    'production_timing': str(row["생산시점"]).strip() if row["생산시점"] else "주야"
                })
                stock += production_qty
            stock -= daily_sales
    
    additional_plan.sort(key=lambda x: (x['deadline'], -x['qty'] * x['sec']))
    production_plan.extend(additional_plan)
    
    # 최종 스케줄 배치
    schedule = {d: {'주간': {}, '야간': {}} for d in DAYS}
    daily_sum = {d: {'주간': 0, '야간': 0} for d in DAYS}
    daily_time = {d: {'주간': 0, '야간': 0} for d in DAYS}
    
    first_week_plan = [p for p in production_plan if not p.get('next_week', False)]
    next_week_plan = [p for p in production_plan if p.get('next_week', False)]
    
    for plan in first_week_plan:
        plan['urgency'] = get_urgency(plan['reason'], plan['product'], 0, False)
    first_week_plan.sort(key=lambda x: -x['urgency'])
    
    for plan in first_week_plan:
        p = plan['product']
        deadline = plan['deadline']
        qty = plan['qty']
        sec = plan['sec']
        reason = plan.get('reason', '')
        urgency = plan['urgency']
        
        placed = False
        valid_days = list(range(deadline + 1))
        valid_days.sort(key=lambda x: (daily_sum[DAYS[x]]['주간'] + daily_sum[DAYS[x]]['야간']))
        
        allowed_shifts = get_allowed_shifts(plan.get('production_timing', '주야'))
        for day_idx in valid_days:
            day = DAYS[day_idx]
            current_urgency = get_urgency(reason, p, deadline - day_idx, False)
            if len(allowed_shifts) == 2:
                shift_preference = ['주간', '야간'] if current_urgency >= 30 else ['야간', '주간']
            else:
                shift_preference = allowed_shifts
            
            for shift in shift_preference:
                if p in schedule[day][shift]:
                    old_qty = schedule[day][shift][p]['qty']
                    new_qty = old_qty + qty
                    new_time = new_qty * sec
                    if daily_sum[day][shift] - old_qty + new_qty <= DAILY_LIMIT and daily_time[day][shift] - (old_qty * sec) + new_time <= WORK_HOURS:
                        daily_sum[day][shift] = daily_sum[day][shift] - old_qty + new_qty
                        daily_time[day][shift] = daily_time[day][shift] - (old_qty * sec) + new_time
                        old_reason = schedule[day][shift][p].get('reason', '')
                        combined_reason = old_reason
                        if reason and reason not in old_reason:
                            combined_reason = (old_reason + " + " + reason) if old_reason else reason
                        schedule[day][shift][p] = {
                            'qty': new_qty, 'sec': sec,
                            'reason': combined_reason, 'urgency': current_urgency
                        }
                        placed = True
                        break
                else:
                    if daily_sum[day][shift] + qty <= DAILY_LIMIT and daily_time[day][shift] + (qty * sec) <= WORK_HOURS:
                        schedule[day][shift][p] = {
                            'qty': qty, 'sec': sec,
                            'reason': reason, 'urgency': current_urgency
                        }
                        daily_sum[day][shift] += qty
                        daily_time[day][shift] += qty * sec
                        placed = True
                        break
            if placed:
                break
    
    for plan in next_week_plan:
        p = plan['product']
        deadline = plan['deadline']
        qty = plan['qty']
        sec = plan['sec']
        reason = plan.get('reason', '')
        
        placed = False
        valid_days = list(range(deadline + 1))
        day_loads = []
        for day_idx in valid_days:
            day = DAYS[day_idx]
            total_qty = daily_sum[day]['주간'] + daily_sum[day]['야간']
            total_time = daily_time[day]['주간'] + daily_time[day]['야간']
            load_score = (total_qty / DAILY_LIMIT) + (total_time / (WORK_HOURS * 2))
            day_loads.append((day_idx, load_score))
        day_loads.sort(key=lambda x: x[1])
        
        allowed_shifts = get_allowed_shifts(plan.get('production_timing', '주야'))
        for day_idx, _ in day_loads:
            day = DAYS[day_idx]
            if len(allowed_shifts) == 2:
                day_load = daily_sum[day]['주간'] / DAILY_LIMIT if DAILY_LIMIT > 0 else 0
                night_load = daily_sum[day]['야간'] / DAILY_LIMIT if DAILY_LIMIT > 0 else 0
                shift_preference = ['주간', '야간'] if day_load <= night_load else ['야간', '주간']
            else:
                shift_preference = allowed_shifts
            
            for shift in shift_preference:
                if p in schedule[day][shift]:
                    old_qty = schedule[day][shift][p]['qty']
                    new_qty = old_qty + qty
                    new_time = new_qty * sec
                    if daily_sum[day][shift] - old_qty + new_qty <= DAILY_LIMIT and daily_time[day][shift] - (old_qty * sec) + new_time <= WORK_HOURS:
                        daily_sum[day][shift] = daily_sum[day][shift] - old_qty + new_qty
                        daily_time[day][shift] = daily_time[day][shift] - (old_qty * sec) + new_time
                        old_reason = schedule[day][shift][p].get('reason', '')
                        combined_reason = old_reason
                        if reason and reason not in old_reason:
                            combined_reason = (old_reason + " + " + reason) if old_reason else reason
                        schedule[day][shift][p] = {
                            'qty': new_qty, 'sec': sec,
                            'reason': combined_reason, 'urgency': 0
                        }
                        placed = True
                        break
                else:
                    if daily_sum[day][shift] + qty <= DAILY_LIMIT and daily_time[day][shift] + (qty * sec) <= WORK_HOURS:
                        schedule[day][shift][p] = {
                            'qty': qty, 'sec': sec,
                            'reason': reason, 'urgency': 0
                        }
                        daily_sum[day][shift] += qty
                        daily_time[day][shift] += qty * sec
                        placed = True
                        break
            if placed:
                break
    
    return schedule, daily_sum, daily_time, date_labels, monday

# ========================
# Supabase DB 함수
# ========================

def delete_schedule(week_start):
    supabase.table("schedules").delete().eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).execute()

def check_schedule_exists(week_start):
    result = supabase.table("schedules").select("id", count="exact").eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).execute()
    return result.count > 0

def save_schedule_to_db(schedule, date_labels, monday):
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
        supabase.table("schedules").insert(rows).execute()

def load_schedule_from_db(week_start):
    result = supabase.table("schedules").select("*").eq(
        "week_start", week_start.strftime('%Y-%m-%d')
    ).order("id").execute()
    if result.data:
        return pd.DataFrame(result.data)
    return pd.DataFrame()

def delete_schedule_row(row_id):
    """단일 행 삭제"""
    supabase.table("schedules").delete().eq("id", row_id).execute()

def update_schedule_row(row_id, day_of_week=None, shift=None, quantity=None, production_time=None):
    """단일 행 수정 (이동 또는 수량 변경)"""
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
        supabase.table("schedules").update(updates).eq("id", row_id).execute()

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
    backup = st.session_state.get('schedule_backup', [])
    if not backup:
        return
    
    # 현재 데이터 전체 삭제
    supabase.table("schedules").delete().eq(
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
            supabase.table("schedules").insert(batch).execute()
    
    st.session_state['schedule_backup'] = []

def get_all_weeks():
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

def generate_schedule_image(df, selected_week):
    """스케줄 데이터를 깔끔한 PNG 이미지로 생성 (Pillow)"""
    
    # 폰트
    font_title = make_font(28, bold=True)
    font_week = make_font(18, bold=True)
    font_summary = make_font(16)
    font_day_header = make_font(20, bold=True)
    font_shift = make_font(16, bold=True)
    font_item = make_font(15)
    font_empty = make_font(14)
    
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
    
    # 레이아웃 상수
    IMG_W = 1100
    PAD_X = 40
    CONTENT_W = IMG_W - PAD_X * 2
    COL_W = CONTENT_W // 2 - 10
    ITEM_H = 28
    DAY_HEADER_H = 44
    SHIFT_HEADER_H = 32
    BLOCK_PAD = 16
    
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
    
    # 전체 높이 계산
    total_h = 60 + 30 + 50 + 20  # title + week + summary + gap
    for day in DAYS:
        d = day_data_map[day]
        rows = max(len(d['day']), len(d['night']), 1)
        total_h += DAY_HEADER_H + SHIFT_HEADER_H + rows * ITEM_H + BLOCK_PAD * 2 + 12
    total_h += 30  # bottom padding
    
    # 이미지 생성
    img = Image.new("RGB", (IMG_W, total_h), BG)
    draw = ImageDraw.Draw(img)
    y = 30
    
    # 타이틀
    title_text = "생산 스케줄"
    bbox = draw.textbbox((0, 0), title_text, font=font_title)
    tw = bbox[2] - bbox[0]
    draw.text(((IMG_W - tw) // 2, y), title_text, fill=TEXT_COLOR, font=font_title)
    y += 42
    
    # 주차 정보
    bbox = draw.textbbox((0, 0), selected_week, font=font_week)
    tw = bbox[2] - bbox[0]
    draw.text(((IMG_W - tw) // 2, y), selected_week, fill="#555555", font=font_week)
    y += 32
    
    # 요약
    total_qty = df['quantity'].sum()
    total_time = df['production_time'].sum()
    total_products = df['product'].nunique()
    summary = f"총 생산량: {total_qty:,}개   |   총 시간: {total_time:.1f}h   |   제품: {total_products}종"
    bbox = draw.textbbox((0, 0), summary, font=font_summary)
    sw = bbox[2] - bbox[0]
    sh = bbox[3] - bbox[1]
    sx = (IMG_W - sw) // 2 - 16
    draw.rounded_rectangle([sx, y - 6, sx + sw + 32, y + sh + 12], radius=8, fill=SUMMARY_BG, outline=SUMMARY_BORDER)
    draw.text(((IMG_W - sw) // 2, y), summary, fill=TEXT_COLOR, font=font_summary)
    y += sh + 30
    
    # 구분선
    draw.line([(PAD_X, y), (IMG_W - PAD_X, y)], fill=DIVIDER, width=1)
    y += 16
    
    # 각 요일
    for day in DAYS:
        data = day_data_map[day]
        num_rows = max(len(data['day']), len(data['night']), 1)
        
        # 요일 헤더
        draw.rounded_rectangle(
            [PAD_X, y, IMG_W - PAD_X, y + DAY_HEADER_H],
            radius=6, fill=HEADER_BG
        )
        label_text = f"  {data['label']}"
        bbox = draw.textbbox((0, 0), label_text, font=font_day_header)
        lw = bbox[2] - bbox[0]
        draw.text(((IMG_W - lw) // 2, y + 10), label_text, fill=HEADER_TEXT, font=font_day_header)
        y += DAY_HEADER_H + 6
        
        block_h = SHIFT_HEADER_H + num_rows * ITEM_H + BLOCK_PAD
        
        # 주간 배경
        left_x = PAD_X
        draw.rounded_rectangle(
            [left_x, y, left_x + COL_W, y + block_h],
            radius=6, fill=DAY_BG, outline=DAY_BORDER
        )
        draw.text((left_x + 12, y + 6), "[주간]", fill="#B8860B", font=font_shift)
        
        # 야간 배경
        right_x = PAD_X + COL_W + 20
        draw.rounded_rectangle(
            [right_x, y, right_x + COL_W, y + block_h],
            radius=6, fill=NIGHT_BG, outline=NIGHT_BORDER
        )
        draw.text((right_x + 12, y + 6), "[야간]", fill="#4A5080", font=font_shift)
        
        item_y = y + SHIFT_HEADER_H + 4
        
        # 주간 항목
        if data['day']:
            for i, item in enumerate(data['day']):
                draw.text((left_x + 16, item_y + i * ITEM_H), f"• {item}", fill=TEXT_COLOR, font=font_item)
        else:
            draw.text((left_x + COL_W // 2 - 30, item_y + (num_rows * ITEM_H) // 2 - 10), "생산 없음", fill=MUTED, font=font_empty)
        
        # 야간 항목
        if data['night']:
            for i, item in enumerate(data['night']):
                draw.text((right_x + 16, item_y + i * ITEM_H), f"• {item}", fill=TEXT_COLOR, font=font_item)
        else:
            draw.text((right_x + COL_W // 2 - 30, item_y + (num_rows * ITEM_H) // 2 - 10), "생산 없음", fill=MUTED, font=font_empty)
        
        y += block_h + 12
    
    # PNG로 저장
    buf = BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# ========================
# 메인 앱
# ========================

st.title("📅 스케줄 관리")

menu = st.radio("선택", ["📅 새 스케줄 생성", "🔍 스케줄 조회", "📈 통계"], horizontal=True)

st.divider()

if menu == "📅 새 스케줄 생성":
    st.header("새 생산 스케줄 생성")
    
    # ── Step 1: 판매 주간 선택
    st.subheader("① 판매 주간 선택")
    sales_date = st.date_input("판매 데이터 주간 (해당 주의 아무 날이나 선택)", datetime.now(), key="sales_date")
    sales_monday = get_week_monday(sales_date)
    sales_friday = sales_monday + timedelta(days=4)
    sales_saturday = sales_monday + timedelta(days=5)
    
    st.info(f"📆 판매 주간: **{sales_monday.strftime('%Y-%m-%d')} (월) ~ {sales_saturday.strftime('%Y-%m-%d')} (토)**")
    
    # 해당 주간 판매 데이터 조회
    sales_df = load_sales_for_week(sales_monday)
    if sales_df.empty:
        st.warning(f"⚠️ {sales_monday.strftime('%Y-%m-%d')} ~ {sales_saturday.strftime('%Y-%m-%d')} 기간의 판매 데이터가 없습니다.")
        st.caption("먼저 '판매 데이터 관리' 페이지에서 해당 기간 데이터를 업로드해주세요.")
    else:
        product_list = get_products_in_sales(sales_df)
        st.success(f"✅ 판매 데이터 {len(sales_df):,}건 조회됨 (제품 {len(product_list)}종)")
    
    # ── Step 2: 스케줄 날짜 선택
    st.subheader("② 스케줄 날짜 선택")
    schedule_date = st.date_input("스케줄에 표시할 주간 (해당 주의 아무 날이나 선택)", datetime.now(), key="schedule_date")
    schedule_monday = get_week_monday(schedule_date)
    schedule_friday = schedule_monday + timedelta(days=4)
    
    st.info(f"📅 스케줄 날짜: **{schedule_monday.strftime('%Y-%m-%d')} (월) ~ {schedule_friday.strftime('%Y-%m-%d')} (금)**")
    
    if not sales_df.empty:
        # ── Step 3: 재고/생산정보 불러오기 (DB 기반)
        st.subheader("③ 재고/생산정보 확인")
        st.caption("📦 재고 → 제품관리 > 재고 탭  |  ⏱️ 개당 생산시간·생산시점·최소생산수량 → 제품관리 > 제품 탭")
        
        inventory_df = load_inventory_from_db()
        
        if inventory_df.empty:
            st.warning("⚠️ 등록된 제품이 없습니다. '제품 관리' 페이지에서 제품을 먼저 등록해주세요.")
        else:
            st.success(f"✅ 제품 {len(inventory_df)}개 로드 완료 (DB 기준)")
            
            # 미리보기
            with st.expander("📋 재고/생산정보 미리보기"):
                st.dataframe(
                    inventory_df[["제품코드", "제품", "현 재고", "개당 생산시간(초)", "생산시점", "최소생산수량"]],
                    use_container_width=True, hide_index=True
                )
            
            # ── Step 4: 제품 선택
            st.subheader("④ 제품 선택")
            
            inv_product_names = [f"{row['제품']} ({row['제품코드']})" for _, row in inventory_df.iterrows()]
            
            col_sel1, col_sel2 = st.columns([1, 1])
            with col_sel1:
                if st.button("✅ 전체 선택"):
                    st.session_state["selected_inv_products"] = inv_product_names
            with col_sel2:
                if st.button("❌ 전체 해제"):
                    st.session_state["selected_inv_products"] = []
            
            default_selection = st.session_state.get("selected_inv_products", inv_product_names)
            default_selection = [n for n in default_selection if n in inv_product_names]
            
            selected_names = st.multiselect(
                "생산할 제품 선택",
                options=inv_product_names,
                default=default_selection,
                placeholder="제품을 선택하세요..."
            )
            
            if selected_names:
                # 선택된 제품만 필터
                selected_codes = []
                for name in selected_names:
                    for _, row in inventory_df.iterrows():
                        label = f"{row['제품']} ({row['제품코드']})"
                        if label == name:
                            selected_codes.append(str(row["제품코드"]).strip())
                            break
                
                filtered_inventory = inventory_df[inventory_df["제품코드"].astype(str).str.strip().isin(selected_codes)].copy()
                
                # ── Step 5: 주간 데이터 확인 & 스케줄 생성
                st.subheader("⑤ 주간 데이터 확인 & 스케줄 생성")
                
                weekly_df, unmatched = build_weekly_data(sales_df, filtered_inventory, sales_monday)
                
                if unmatched:
                    st.warning(f"⚠️ 판매 데이터에 매칭되지 않는 제품 {len(unmatched)}개: {', '.join(unmatched[:10])}{'...' if len(unmatched) > 10 else ''}")
                
                if not weekly_df.empty:
                    preview_cols = ["제품", "제품코드", "현 재고", "월", "화", "수", "목", "금", "토", "개당 생산시간(초)", "생산시점", "최소생산수량"]
                    available_cols = [c for c in preview_cols if c in weekly_df.columns]
                    st.dataframe(
                        weekly_df[available_cols],
                        use_container_width=True,
                        hide_index=True
                    )
                    st.caption(f"매칭된 제품: {len(weekly_df)}개")
                    
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
                                                    data.append({
                                                        '순서': i, '제품': p,
                                                        '수량': f"{info['qty']}개",
                                                        '시간': f"{round(info['qty'] * info['sec'] / 3600, 1)}h",
                                                        '이유': info['reason']
                                                    })
                                                st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
                                                st.caption(f"생산량: {daily_sum[day]['주간']}/{DAILY_LIMIT}개 ({round(daily_sum[day]['주간']/DAILY_LIMIT*100, 1)}%)")
                                            else:
                                                st.info("생산 없음")
                                        
                                        with col2:
                                            st.markdown("**🌙 야간**")
                                            if schedule[day]['야간']:
                                                data = []
                                                for i, (p, info) in enumerate(schedule[day]['야간'].items(), 1):
                                                    data.append({
                                                        '순서': i, '제품': p,
                                                        '수량': f"{info['qty']}개",
                                                        '시간': f"{round(info['qty'] * info['sec'] / 3600, 1)}h",
                                                        '이유': info['reason']
                                                    })
                                                st.dataframe(pd.DataFrame(data), use_container_width=True, hide_index=True)
                                                st.caption(f"생산량: {daily_sum[day]['야간']}/{DAILY_LIMIT}개 ({round(daily_sum[day]['야간']/DAILY_LIMIT*100, 1)}%)")
                                            else:
                                                st.info("생산 없음")
                                        
                                        st.divider()
                                    
                                except Exception as e:
                                    st.error(f"❌ 오류 발생: {str(e)}")
                else:
                    st.warning("매칭되는 제품이 없습니다. 제품관리에서 제품코드를 확인해주세요.")

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
            df = load_schedule_from_db(week_start)
            
            if not df.empty:
                # 수정 모드 토글 (주차별로 저장, 주차 변경 시 초기화)
                is_edit_mode = st.session_state.get('schedule_edit_week') == selected_week and st.session_state.get('schedule_edit_mode', False)
                
                # 상단 버튼 배치: 수정/완료/취소(왼쪽) + 다운로드(오른쪽)
                col_edit_btn, col_cancel_btn, col_del_btn, _, col_dl_excel, col_dl_img = st.columns([1, 1, 1, 0.5, 1, 1])
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
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='생산스케줄')
                    st.download_button(
                        label="📥 엑셀 다운로드",
                        data=output.getvalue(),
                        file_name=f"생산스케줄_{selected_week.replace(' ~ ', '_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_excel"
                    )
                with col_dl_img:
                    try:
                        img_buf = generate_schedule_image(df, selected_week)
                        st.download_button(
                            label="📸 스크린샷 저장",
                            data=img_buf.getvalue(),
                            file_name=f"생산스케줄_{selected_week.replace(' ~ ', '_')}.png",
                            mime="image/png",
                            key="download_screenshot"
                        )
                    except Exception as e:
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
                        
                        # 요일 라벨 목록 (DB에 저장된 형태)
                        day_labels_list = df['day_of_week'].drop_duplicates().tolist()
                        if not day_labels_list:
                            day_labels_list = [f"({d})" for d in DAYS]
                        
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
                                        "week_start": week_start.strftime('%Y-%m-%d'),
                                        "week_end": week_end.strftime('%Y-%m-%d'),
                                        "day_of_week": add_day,
                                        "shift": add_shift,
                                        "product": final_name,
                                        "quantity": int(add_quantity),
                                        "production_time": round(float(add_production_time), 1),
                                        "reason": add_reason.strip() if add_reason else "수동 추가",
                                        "urgency": 0
                                    }
                                    supabase.table("schedules").insert(new_row).execute()
                                    st.success(f"✅ **{final_name}** {int(add_quantity)}개 → {add_day} {add_shift}에 추가되었습니다.")
                                    load_all_product_names.clear()
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"❌ 추가 실패: {str(e)}")
                
                if not is_edit_mode:
                    # 기존 보기 모드: 데이터프레임으로 표시
                    for day in DAYS:
                        day_matches = df[df['day_of_week'].str.contains(day)]
                        day_label = day_matches['day_of_week'].iloc[0] if len(day_matches) > 0 else f"({day})"
                        st.subheader(f"▶ {day_label}")
                        day_data = df[df['day_of_week'].str.contains(day)]
                        
                        if not day_data.empty:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**🌞 주간**")
                                day_shift = day_data[day_data['shift'] == '주간']
                                if not day_shift.empty:
                                    st.dataframe(
                                        day_shift[['product', 'quantity', 'production_time', 'reason']].rename(columns={
                                            'product': '제품', 'quantity': '수량(개)',
                                            'production_time': '시간(h)', 'reason': '이유'
                                        }),
                                        use_container_width=True, hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                            with col2:
                                st.markdown("**🌙 야간**")
                                night_shift = day_data[day_data['shift'] == '야간']
                                if not night_shift.empty:
                                    st.dataframe(
                                        night_shift[['product', 'quantity', 'production_time', 'reason']].rename(columns={
                                            'product': '제품', 'quantity': '수량(개)',
                                            'production_time': '시간(h)', 'reason': '이유'
                                        }),
                                        use_container_width=True, hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                        else:
                            st.info("생산 없음")
                        st.divider()
                else:
                    # 수정 모드: 삭제/이동/수량수정 버튼 표시
                    day_labels = df['day_of_week'].drop_duplicates().tolist()
                    for day in DAYS:
                        day_matches = df[df['day_of_week'].str.contains(day)]
                        day_label = day_matches['day_of_week'].iloc[0] if len(day_matches) > 0 else f"({day})"
                        st.subheader(f"▶ {day_label}")
                        day_data = df[df['day_of_week'].str.contains(day)]
                        
                        if not day_data.empty:
                            col1, col2 = st.columns(2)
                            with col1:
                                st.markdown("**🌞 주간**")
                                shift_data = day_data[day_data['shift'] == '주간']
                                if not shift_data.empty:
                                    for _, row in shift_data.iterrows():
                                        with st.container():
                                            c_del, c_name, c_qty, c_day, c_shift, c_apply = st.columns([0.5, 2.5, 1.2, 1.8, 1, 0.8])
                                            with c_del:
                                                if st.button("🗑️", key=f"del_{row['id']}", help="삭제"):
                                                    delete_schedule_row(row['id'])
                                                    st.rerun()
                                            with c_name:
                                                st.caption(f"**{row['product']}**\n{row['production_time']}h · {row.get('reason', '')}")
                                            with c_qty:
                                                new_qty = st.number_input("수량", min_value=1, value=int(row['quantity']), step=1, key=f"qty_{row['id']}", label_visibility="collapsed")
                                            with c_day:
                                                current_day_idx = day_labels.index(row['day_of_week']) if row['day_of_week'] in day_labels else 0
                                                move_day = st.selectbox("요일", day_labels, index=current_day_idx, key=f"move_day_{row['id']}", label_visibility="collapsed")
                                            with c_shift:
                                                current_shift_idx = 0 if row['shift'] == '주간' else 1
                                                move_shift = st.selectbox("교대", ["주간", "야간"], index=current_shift_idx, key=f"move_shift_{row['id']}", label_visibility="collapsed")
                                            with c_apply:
                                                if st.button("적용", key=f"apply_{row['id']}"):
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
                                                        update_schedule_row(row['id'], **updates_kw)
                                                        st.rerun()
                                else:
                                    st.info("생산 없음")
                            with col2:
                                st.markdown("**🌙 야간**")
                                shift_data = day_data[day_data['shift'] == '야간']
                                if not shift_data.empty:
                                    for _, row in shift_data.iterrows():
                                        with st.container():
                                            c_del, c_name, c_qty, c_day, c_shift, c_apply = st.columns([0.5, 2.5, 1.2, 1.8, 1, 0.8])
                                            with c_del:
                                                if st.button("🗑️", key=f"del_{row['id']}", help="삭제"):
                                                    delete_schedule_row(row['id'])
                                                    st.rerun()
                                            with c_name:
                                                st.caption(f"**{row['product']}**\n{row['production_time']}h · {row.get('reason', '')}")
                                            with c_qty:
                                                new_qty = st.number_input("수량", min_value=1, value=int(row['quantity']), step=1, key=f"qty_{row['id']}", label_visibility="collapsed")
                                            with c_day:
                                                current_day_idx = day_labels.index(row['day_of_week']) if row['day_of_week'] in day_labels else 0
                                                move_day = st.selectbox("요일", day_labels, index=current_day_idx, key=f"move_day_{row['id']}", label_visibility="collapsed")
                                            with c_shift:
                                                current_shift_idx = 0 if row['shift'] == '주간' else 1
                                                move_shift = st.selectbox("교대", ["주간", "야간"], index=current_shift_idx, key=f"move_shift_{row['id']}", label_visibility="collapsed")
                                            with c_apply:
                                                if st.button("적용", key=f"apply_{row['id']}"):
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
                                                        update_schedule_row(row['id'], **updates_kw)
                                                        st.rerun()
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
            df = load_schedule_from_db(week_start)
            
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

