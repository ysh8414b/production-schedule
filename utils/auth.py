import streamlit as st
from supabase import create_client


# ========================
# Supabase 클라이언트
# ========================

@st.cache_resource
def _get_anon_client():
    """읽기 전용 anon 클라이언트 (캐시됨)"""
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


def get_supabase_client():
    """로그인 상태면 인증된 클라이언트, 아니면 anon 클라이언트 반환"""
    session = st.session_state.get("auth_session")
    if session:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        client = create_client(url, key)
        client.auth.set_session(session.access_token, session.refresh_token)
        return client
    return _get_anon_client()


def get_admin_client():
    """service_role 키를 사용하는 관리자 클라이언트 (사용자 관리용)"""
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)


# ========================
# 인증 함수
# ========================

def login(email: str, password: str):
    """이메일/비밀번호로 로그인, 세션을 session_state에 저장"""
    client = _get_anon_client()
    response = client.auth.sign_in_with_password({
        "email": email,
        "password": password,
    })
    st.session_state["auth_session"] = response.session
    st.session_state["auth_user"] = response.user


def logout():
    """로그아웃 - 세션 클리어"""
    st.session_state.pop("auth_session", None)
    st.session_state.pop("auth_user", None)


def is_authenticated() -> bool:
    """현재 로그인 상태인지 확인"""
    return st.session_state.get("auth_session") is not None


def is_admin() -> bool:
    """현재 사용자가 관리자인지 확인 (app_metadata.role == 'admin')"""
    user = st.session_state.get("auth_user")
    if not user:
        return False
    metadata = getattr(user, "app_metadata", None) or {}
    return metadata.get("role") == "admin"


# ========================
# 탭별 권한 함수
# ========================

# 권한 관리 대상 탭 정의
TAB_KEYS = {
    "schedule": "스케줄 관리",
    "products": "제품 관리",
    "sales": "판매 데이터",
    "loss_data": "로스 데이터",
    "loading": "적재리스트",
}


def get_user_permission(tab_key: str) -> str:
    """현재 사용자의 특정 탭 권한 반환 ('edit' / 'view' / 'none')
    - 관리자 → 항상 'edit'
    - 비로그인 → 항상 'view'
    - 일반 사용자 → app_metadata.permissions에서 조회, 기본값 'view'
    """
    if is_admin():
        return "edit"
    user = st.session_state.get("auth_user")
    if not user:
        return "view"
    metadata = getattr(user, "app_metadata", None) or {}
    permissions = metadata.get("permissions", {})
    return permissions.get(tab_key, "view")


def can_edit(tab_key: str) -> bool:
    """해당 탭에서 편집 가능 여부"""
    return get_user_permission(tab_key) == "edit"


def can_access(tab_key: str) -> bool:
    """해당 탭 접근 가능 여부 (none이 아닌지)"""
    return get_user_permission(tab_key) != "none"
