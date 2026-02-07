import streamlit as st
import pandas as pd
import sqlite3
import math
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from io import BytesIO

# ========================
# 페이지 설정
# ========================

st.set_page_config(
    page_title="생산 스케줄 관리",
    page_icon="📊",
    layout="wide"
)

# ========================
# 데이터베이스 초기화
# ========================

def init_db():
    """데이터베이스 초기화"""
    conn = sqlite3.connect('production_schedule.db')
    c = conn.cursor()
    
    # 스케줄 테이블
    c.execute('''
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            week_start DATE,
            week_end DATE,
            day_of_week TEXT,
            shift TEXT,
            product TEXT,
            quantity INTEGER,
            production_time REAL,
            reason TEXT,
            urgency INTEGER
        )
    ''')
    
    conn.commit()
    conn.close()

init_db()

# ========================
# 설정 상수
# ========================

DAYS = ["월", "화", "수", "목", "금"]
DAILY_LIMIT = 200
WORK_HOURS = 8 * 60 * 60
BATCH_SIZE = 40

# ========================
# 유틸리티 함수
# ========================

def get_week_monday(selected_date):
    """선택한 날짜가 속한 주의 월요일 찾기"""
    weekday = selected_date.weekday()
    days_to_monday = weekday
    monday = selected_date - timedelta(days=days_to_monday)
    return monday

def get_urgency(reason, product, deadline_days, is_next_week):
    """긴급도 계산"""
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

def create_schedule(sales_file, start_date):
    """스케줄 생성 메인 로직"""
    df = pd.read_excel(sales_file)
    df["주간판매"] = df[DAYS].sum(axis=1)
    df = df[df["주간판매"] > 0].copy()
    df = df[df["현 재고"].notna()].copy()
    df["개당 생산시간(초)"] = df["개당 생산시간(초)"].fillna(0)
    
    monday = get_week_monday(start_date)
    date_labels = {}
    for i, d in enumerate(DAYS):
        current_date = monday + timedelta(days=i)
        date_labels[d] = f"{current_date.strftime('%m/%d')} ({d})"
    
    # ========================
    # 1차 생산 계획 (이번 주 필수)
    # ========================
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
                future_sales = daily_sales + row["토"] + row["월"] + row["화"]
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
                    'next_week': False
                })
                
                stock += production_qty
            
            stock -= daily_sales
    
    # ========================
    # 2차 생산 계획 (다음 주 대비)
    # ========================
    
    # 임시 배치로 1차 계획 용량 확인
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
        for day_idx in valid_days:
            day = DAYS[day_idx]
            for shift in ['주간', '야간']:
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
    
    # 이번 주 마지막 재고 상태 계산
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
    
    # 다음 주 대비 추가 생산
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
                future_sales = daily_sales + row["토"] + row["월"]
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
                    'next_week': True
                })
                
                stock += production_qty
            
            stock -= daily_sales
    
    additional_plan.sort(key=lambda x: (x['deadline'], -x['qty'] * x['sec']))
    production_plan.extend(additional_plan)
    
    # ========================
    # 스케줄 배치 (주간/야간)
    # ========================
    schedule = {d: {'주간': {}, '야간': {}} for d in DAYS}
    daily_sum = {d: {'주간': 0, '야간': 0} for d in DAYS}
    daily_time = {d: {'주간': 0, '야간': 0} for d in DAYS}
    
    # 1차 작업과 2차 작업 분리
    first_week_plan = [p for p in production_plan if not p.get('next_week', False)]
    next_week_plan = [p for p in production_plan if p.get('next_week', False)]
    
    # ========================
    # 1단계: 이번 주 필수 작업 배치 (긴급도 기준)
    # ========================
    
    # 긴급도 계산
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
        
        for day_idx in valid_days:
            day = DAYS[day_idx]
            current_urgency = get_urgency(reason, p, deadline - day_idx, False)
            shift_preference = ['주간', '야간'] if current_urgency >= 30 else ['야간', '주간']
            
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
                            'qty': new_qty, 
                            'sec': sec, 
                            'reason': combined_reason,
                            'urgency': current_urgency
                        }
                        placed = True
                        break
                else:
                    if daily_sum[day][shift] + qty <= DAILY_LIMIT and daily_time[day][shift] + (qty * sec) <= WORK_HOURS:
                        schedule[day][shift][p] = {
                            'qty': qty, 
                            'sec': sec, 
                            'reason': reason,
                            'urgency': current_urgency
                        }
                        daily_sum[day][shift] += qty
                        daily_time[day][shift] += qty * sec
                        placed = True
                        break
            
            if placed:
                break
    
    # ========================
    # 2단계: 다음주 대비 작업 배치 (부하 균등 분산)
    # ========================
    
    # 다음주 작업은 부하가 적은 날부터 채움
    for plan in next_week_plan:
        p = plan['product']
        deadline = plan['deadline']
        qty = plan['qty']
        sec = plan['sec']
        reason = plan.get('reason', '')
        
        placed = False
        
        # 마감일까지의 모든 요일을 부하가 적은 순으로 정렬
        valid_days = list(range(deadline + 1))
        
        # 각 날짜의 총 부하 계산 (주간+야간)
        day_loads = []
        for day_idx in valid_days:
            day = DAYS[day_idx]
            total_qty = daily_sum[day]['주간'] + daily_sum[day]['야간']
            total_time = daily_time[day]['주간'] + daily_time[day]['야간']
            # 수량 여유와 시간 여유를 함께 고려
            load_score = (total_qty / DAILY_LIMIT) + (total_time / (WORK_HOURS * 2))
            day_loads.append((day_idx, load_score))
        
        # 부하가 적은 순으로 정렬
        day_loads.sort(key=lambda x: x[1])
        
        for day_idx, _ in day_loads:
            day = DAYS[day_idx]
            
            # 주간/야간 중 더 비어있는 곳 선택
            day_load = daily_sum[day]['주간'] / DAILY_LIMIT
            night_load = daily_sum[day]['야간'] / DAILY_LIMIT
            
            # 부하가 적은 시간대 우선
            if day_load <= night_load:
                shift_preference = ['주간', '야간']
            else:
                shift_preference = ['야간', '주간']
            
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
                            'qty': new_qty, 
                            'sec': sec, 
                            'reason': combined_reason,
                            'urgency': 0  # 다음주는 긴급도 낮음
                        }
                        placed = True
                        break
                else:
                    if daily_sum[day][shift] + qty <= DAILY_LIMIT and daily_time[day][shift] + (qty * sec) <= WORK_HOURS:
                        schedule[day][shift][p] = {
                            'qty': qty, 
                            'sec': sec, 
                            'reason': reason,
                            'urgency': 0  # 다음주는 긴급도 낮음
                        }
                        daily_sum[day][shift] += qty
                        daily_time[day][shift] += qty * sec
                        placed = True
                        break
            
            if placed:
                break
    
    return schedule, daily_sum, daily_time, date_labels, monday

def delete_schedule(week_start):
    """특정 주차 스케줄 삭제"""
    conn = sqlite3.connect('production_schedule.db')
    c = conn.cursor()
    c.execute("DELETE FROM schedules WHERE week_start = ?", (week_start.strftime('%Y-%m-%d'),))
    conn.commit()
    conn.close()

def check_schedule_exists(week_start):
    """스케줄 존재 여부 확인"""
    conn = sqlite3.connect('production_schedule.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM schedules WHERE week_start = ?", (week_start.strftime('%Y-%m-%d'),))
    count = c.fetchone()[0]
    conn.close()
    return count > 0

def save_schedule_to_db(schedule, date_labels, monday):
    """스케줄을 데이터베이스에 저장"""
    conn = sqlite3.connect('production_schedule.db')
    c = conn.cursor()
    
    friday = monday + timedelta(days=4)
    
    for day in DAYS:
        for shift in ['주간', '야간']:
            for product, data in schedule[day][shift].items():
                c.execute('''
                    INSERT INTO schedules (week_start, week_end, day_of_week, shift, product, quantity, production_time, reason, urgency)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    monday.strftime('%Y-%m-%d'),
                    friday.strftime('%Y-%m-%d'),
                    date_labels[day],
                    shift,
                    product,
                    data['qty'],
                    round(data['qty'] * data['sec'] / 3600, 1),
                    data['reason'],
                    data['urgency']
                ))
    
    conn.commit()
    conn.close()

def load_schedule_from_db(week_start):
    """데이터베이스에서 스케줄 불러오기"""
    conn = sqlite3.connect('production_schedule.db')
    df = pd.read_sql_query(
        "SELECT * FROM schedules WHERE week_start = ? ORDER BY id",
        conn,
        params=(week_start.strftime('%Y-%m-%d'),)
    )
    conn.close()
    return df

def get_all_weeks():
    """저장된 모든 주차 목록"""
    conn = sqlite3.connect('production_schedule.db')
    c = conn.cursor()
    c.execute("SELECT DISTINCT week_start, week_end FROM schedules ORDER BY week_start DESC")
    weeks = c.fetchall()
    conn.close()
    return weeks

# ========================
# 메인 앱
# ========================

st.title("📊 생산 스케줄 관리 시스템")

# 사이드바
with st.sidebar:
    st.header("⚙️ 메뉴")
    menu = st.radio("선택", ["📅 새 스케줄 생성", "🔍 스케줄 조회", "📈 통계"])

# ========================
# 1. 새 스케줄 생성
# ========================

if menu == "📅 새 스케줄 생성":
    st.header("새 생산 스케줄 생성")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        selected_date = st.date_input("날짜 선택", datetime.now())
        uploaded_file = st.file_uploader("📁 판매 데이터 업로드 (Excel)", type=['xlsx'])
    
    # 중복 체크
    if uploaded_file:
        monday = get_week_monday(selected_date)
        friday = monday + timedelta(days=4)
        
        exists = check_schedule_exists(monday)
        
        if exists:
            st.warning(f"⚠️ **{monday.strftime('%Y-%m-%d')} ~ {friday.strftime('%Y-%m-%d')}** 주차 스케줄이 이미 존재합니다!")
            
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
    
    # 스케줄 생성 버튼
    if uploaded_file and st.session_state.get('confirm_delete', False):
        if st.button("🚀 스케줄 생성", type="primary", key="create_schedule"):
            with st.spinner("스케줄 생성 중..."):
                try:
                    # 기존 스케줄 삭제
                    monday = get_week_monday(selected_date)
                    if check_schedule_exists(monday):
                        delete_schedule(monday)
                        st.success("✅ 기존 스케줄 삭제 완료")
                    
                    # 새 스케줄 생성
                    schedule, daily_sum, daily_time, date_labels, monday = create_schedule(uploaded_file, selected_date)
                    save_schedule_to_db(schedule, date_labels, monday)
                    
                    st.success(f"✅ 스케줄 생성 완료! ({date_labels['월']} ~ {date_labels['금']})")
                    
                    # 세션 상태 초기화
                    st.session_state['confirm_delete'] = False
                    
                    # 결과 표시
                    for day in DAYS:
                        st.subheader(f"▶ {date_labels[day]}")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**🌞 주간**")
                            if schedule[day]['주간']:
                                data = []
                                for i, (p, info) in enumerate(schedule[day]['주간'].items(), 1):
                                    data.append({
                                        '순서': i,
                                        '제품': p,
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
                                        '순서': i,
                                        '제품': p,
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

# ========================
# 2. 스케줄 조회
# ========================

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
                # 요일별 탭
                tabs = st.tabs([df[df['day_of_week'].str.contains(d)]['day_of_week'].iloc[0] if len(df[df['day_of_week'].str.contains(d)]) > 0 else f"0 ({d})" for d in DAYS])
                
                for i, day in enumerate(DAYS):
                    with tabs[i]:
                        day_data = df[df['day_of_week'].str.contains(day)]
                        
                        if not day_data.empty:
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown("**🌞 주간**")
                                day_shift = day_data[day_data['shift'] == '주간']
                                if not day_shift.empty:
                                    st.dataframe(
                                        day_shift[['product', 'quantity', 'production_time', 'reason']].rename(columns={
                                            'product': '제품',
                                            'quantity': '수량(개)',
                                            'production_time': '시간(h)',
                                            'reason': '이유'
                                        }),
                                        use_container_width=True,
                                        hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                            
                            with col2:
                                st.markdown("**🌙 야간**")
                                night_shift = day_data[day_data['shift'] == '야간']
                                if not night_shift.empty:
                                    st.dataframe(
                                        night_shift[['product', 'quantity', 'production_time', 'reason']].rename(columns={
                                            'product': '제품',
                                            'quantity': '수량(개)',
                                            'production_time': '시간(h)',
                                            'reason': '이유'
                                        }),
                                        use_container_width=True,
                                        hide_index=True
                                    )
                                else:
                                    st.info("생산 없음")
                
                # 엑셀 다운로드
                st.divider()
                if st.button("📥 엑셀로 다운로드"):
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name='생산스케줄')
                    
                    st.download_button(
                        label="💾 Excel 파일 다운로드",
                        data=output.getvalue(),
                        file_name=f"생산스케줄_{selected_week.replace(' ~ ', '_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )

# ========================
# 3. 통계
# ========================

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
                    # 요일별 생산량
                    daily_qty = df.groupby('day_of_week')['quantity'].sum().reset_index()
                    fig1 = px.bar(daily_qty, x='day_of_week', y='quantity', 
                                 title='요일별 총 생산량',
                                 labels={'day_of_week': '요일', 'quantity': '생산량(개)'},
                                 color='quantity',
                                 color_continuous_scale='Blues')
                    st.plotly_chart(fig1, use_container_width=True)
                
                with col2:
                    # 주간/야간 비율
                    shift_qty = df.groupby('shift')['quantity'].sum().reset_index()
                    fig2 = px.pie(shift_qty, values='quantity', names='shift',
                                 title='주간/야간 생산 비율',
                                 color='shift',
                                 color_discrete_map={'주간': '#1f77b4', '야간': '#ff7f0e'})
                    st.plotly_chart(fig2, use_container_width=True)
                
                # 제품별 생산량 TOP 10
                product_qty = df.groupby('product')['quantity'].sum().reset_index().sort_values('quantity', ascending=False).head(10)
                fig3 = px.bar(product_qty, x='quantity', y='product', orientation='h',
                             title='제품별 생산량 TOP 10',
                             labels={'product': '제품', 'quantity': '생산량(개)'},
                             color='quantity',
                             color_continuous_scale='Greens')
                st.plotly_chart(fig3, use_container_width=True)
                
                # 주간 요약
                st.subheader("📊 주간 요약")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("총 생산량", f"{df['quantity'].sum()}개")
                
                with col2:
                    st.metric("총 생산시간", f"{df['production_time'].sum():.1f}시간")
                
                with col3:
                    st.metric("제품 종류", f"{df['product'].nunique()}개")
                
                with col4:
                    avg_urgency = df['urgency'].mean()
                    st.metric("평균 긴급도", f"{avg_urgency:.0f}점")

st.sidebar.divider()
st.sidebar.caption("v1.0.0 | 생산 스케줄 관리 시스템")
