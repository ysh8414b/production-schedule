import streamlit as st
from utils.auth import is_authenticated, get_admin_client

# ========================
# 접근 권한 체크
# ========================

if not is_authenticated():
    st.error("로그인이 필요합니다.")
    st.stop()

st.title("관리자 페이지")

# ========================
# 사용자 관리
# ========================

admin_client = get_admin_client()

# ── 새 사용자 생성 ──
st.subheader("사용자 추가")
with st.form("create_user_form"):
    new_email = st.text_input("이메일")
    new_password = st.text_input("비밀번호", type="password")
    new_password_confirm = st.text_input("비밀번호 확인", type="password")
    create_submitted = st.form_submit_button("사용자 생성", use_container_width=True)

    if create_submitted:
        if not new_email or not new_password:
            st.error("이메일과 비밀번호를 입력하세요.")
        elif new_password != new_password_confirm:
            st.error("비밀번호가 일치하지 않습니다.")
        elif len(new_password) < 6:
            st.error("비밀번호는 6자 이상이어야 합니다.")
        else:
            try:
                admin_client.auth.admin.create_user({
                    "email": new_email,
                    "password": new_password,
                    "email_confirm": True,
                })
                st.success(f"사용자 생성 완료: {new_email}")
                st.rerun()
            except Exception as e:
                error_msg = str(e)
                if "already been registered" in error_msg:
                    st.error("이미 등록된 이메일입니다.")
                else:
                    st.error(f"사용자 생성 실패: {error_msg}")

# ── 사용자 목록 ──
st.divider()
st.subheader("등록된 사용자")

try:
    users_response = admin_client.auth.admin.list_users()
    users = users_response if isinstance(users_response, list) else getattr(users_response, "users", [])

    if users:
        for user in users:
            email = getattr(user, "email", str(user))
            user_id = getattr(user, "id", None)
            created = getattr(user, "created_at", "")
            last_sign_in = getattr(user, "last_sign_in_at", "미접속")

            with st.container(border=True):
                col1, col2, col3 = st.columns([3, 2, 1])
                with col1:
                    st.write(f"**{email}**")
                with col2:
                    if last_sign_in and last_sign_in != "미접속":
                        st.caption(f"마지막 접속: {str(last_sign_in)[:10]}")
                    else:
                        st.caption("미접속")
                with col3:
                    current_user = st.session_state.get("auth_user")
                    if current_user and getattr(current_user, "id", None) == user_id:
                        st.caption("(나)")
                    else:
                        if st.button("삭제", key=f"del_user_{user_id}", type="secondary"):
                            st.session_state[f"_confirm_delete_{user_id}"] = True

                if st.session_state.get(f"_confirm_delete_{user_id}"):
                    st.warning(f"정말 **{email}** 사용자를 삭제하시겠습니까?")
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("삭제 확인", key=f"confirm_del_{user_id}", type="primary"):
                            try:
                                admin_client.auth.admin.delete_user(user_id)
                                st.session_state.pop(f"_confirm_delete_{user_id}", None)
                                st.success(f"{email} 삭제 완료")
                                st.rerun()
                            except Exception as e:
                                st.error(f"삭제 실패: {str(e)}")
                    with c2:
                        if st.button("취소", key=f"cancel_del_{user_id}"):
                            st.session_state.pop(f"_confirm_delete_{user_id}", None)
                            st.rerun()
    else:
        st.info("등록된 사용자가 없습니다.")
except Exception as e:
    st.error(f"사용자 목록 조회 실패: {str(e)}")
    st.caption("SUPABASE_SERVICE_ROLE_KEY가 올바르게 설정되어 있는지 확인하세요.")
