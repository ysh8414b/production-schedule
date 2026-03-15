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
