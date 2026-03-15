import streamlit as st
from utils.auth import is_authenticated, is_admin, get_admin_client

# ========================
# 접근 권한 체크
# ========================

if not is_authenticated():
    st.error("로그인이 필요합니다.")
    st.stop()

if not is_admin():
    st.error("관리자 권한이 필요합니다.")
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
    is_admin_user = st.checkbox("관리자 권한 부여")
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
                user_data = {
                    "email": new_email,
                    "password": new_password,
                    "email_confirm": True,
                }
                if is_admin_user:
                    user_data["app_metadata"] = {"role": "admin"}
                admin_client.auth.admin.create_user(user_data)
                role_label = "관리자" if is_admin_user else "일반 사용자"
                st.success(f"사용자 생성 완료: {new_email} ({role_label})")
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
            last_sign_in = getattr(user, "last_sign_in_at", "미접속")
            app_metadata = getattr(user, "app_metadata", None) or {}
            user_role = app_metadata.get("role", "")
            is_user_admin = user_role == "admin"

            current_user = st.session_state.get("auth_user")
            is_me = current_user and getattr(current_user, "id", None) == user_id

            with st.container(border=True):
                col1, col2, col3, col4 = st.columns([3, 1, 2, 1])
                with col1:
                    label = f"**{email}**"
                    if is_me:
                        label += " (나)"
                    st.write(label)
                with col2:
                    if is_user_admin:
                        st.caption("🔐 관리자")
                    else:
                        st.caption("👤 일반")
                with col3:
                    if last_sign_in and last_sign_in != "미접속":
                        st.caption(f"마지막 접속: {str(last_sign_in)[:10]}")
                    else:
                        st.caption("미접속")
                with col4:
                    if not is_me:
                        # 역할 변경 버튼
                        if is_user_admin:
                            if st.button("권한 해제", key=f"demote_{user_id}", type="secondary"):
                                try:
                                    admin_client.auth.admin.update_user_by_id(
                                        user_id, {"app_metadata": {"role": "user"}}
                                    )
                                    st.success(f"{email} → 일반 사용자")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"변경 실패: {str(e)}")
                        else:
                            if st.button("관리자", key=f"promote_{user_id}", type="secondary"):
                                try:
                                    admin_client.auth.admin.update_user_by_id(
                                        user_id, {"app_metadata": {"role": "admin"}}
                                    )
                                    st.success(f"{email} → 관리자")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"변경 실패: {str(e)}")

                # 삭제 기능
                if not is_me:
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
                        if st.button("🗑️ 삭제", key=f"del_user_{user_id}", type="secondary"):
                            st.session_state[f"_confirm_delete_{user_id}"] = True
                            st.rerun()
    else:
        st.info("등록된 사용자가 없습니다.")
except Exception as e:
    st.error(f"사용자 목록 조회 실패: {str(e)}")
    st.caption("SUPABASE_SERVICE_ROLE_KEY가 올바르게 설정되어 있는지 확인하세요.")
