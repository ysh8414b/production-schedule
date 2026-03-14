import streamlit as st
import pandas as pd
import uuid
from views.products import supabase
from utils.auth import is_authenticated


# ========================
# 브랜드 DB 함수
# ========================

BUCKET_NAME = "brand-images"


def _ensure_bucket():
    """Storage 버킷 존재 확인 - 항상 True 반환하고 실제 업로드에서 에러 처리"""
    return True, None


@st.cache_data(ttl=60)
def _has_image_column():
    """image_url 컬럼 존재 여부 확인 (1분 캐시)"""
    try:
        supabase.table("brands").select("image_url").limit(1).execute()
        return True
    except:
        return False


def load_brands():
    """brands 테이블에서 브랜드 목록 로드"""
    try:
        result = supabase.table("brands").select("*").order("name").execute()
        if result.data:
            return pd.DataFrame(result.data)
    except:
        pass
    return pd.DataFrame(columns=["id", "name", "description", "memo"])


def upsert_brand(name, description="", memo="", image_url=None):
    data = {
        "name": str(name).strip(),
        "description": str(description).strip() if description else "",
        "memo": str(memo).strip() if memo else ""
    }
    if image_url is not None and _has_image_column():
        data["image_url"] = image_url
    supabase.table("brands").upsert(
        data,
        on_conflict="name"
    ).execute()


def update_brand_image(brand_name, image_url):
    """브랜드 이미지 URL만 업데이트"""
    if not _has_image_column():
        return
    supabase.table("brands").update(
        {"image_url": image_url}
    ).eq("name", brand_name).execute()


def delete_brand(brand_id):
    supabase.table("brands").delete().eq("id", brand_id).execute()


# ========================
# 이미지 스토리지 함수
# ========================

def upload_brand_image(file, brand_name):
    """브랜드 이미지를 Supabase Storage에 업로드하고 공개 URL 반환"""
    try:
        # 파일명 생성 (브랜드명 + UUID)
        ext = file.name.split(".")[-1] if "." in file.name else "png"
        safe_name = brand_name.replace(" ", "_").replace("/", "_")
        file_path = f"{safe_name}_{uuid.uuid4().hex[:8]}.{ext}"

        # 기존 이미지 삭제 (같은 브랜드의 이전 이미지)
        try:
            existing = supabase.storage.from_(BUCKET_NAME).list()
            if isinstance(existing, list):
                for item in existing:
                    if item.get("name", "").startswith(safe_name + "_"):
                        supabase.storage.from_(BUCKET_NAME).remove([item["name"]])
        except:
            pass

        # 업로드
        file_bytes = file.read()
        result = supabase.storage.from_(BUCKET_NAME).upload(
            file_path,
            file_bytes,
            {"content-type": file.type or "image/png"}
        )

        # 업로드 결과 확인
        if hasattr(result, 'json') and isinstance(result.json(), dict):
            json_result = result.json()
            if 'error' in json_result or 'statusCode' in json_result:
                error_msg = json_result.get('message', json_result.get('error', str(json_result)))
                return None, f"STORAGE_POLICY:{error_msg}"

        # 공개 URL 가져오기
        public_url = supabase.storage.from_(BUCKET_NAME).get_public_url(file_path)
        return public_url, None

    except Exception as e:
        error_str = str(e)
        # Storage 정책 관련 에러 감지
        if "Bucket not found" in error_str or "404" in error_str:
            return None, f"STORAGE_POLICY:{error_str}"
        if "row-level security" in error_str.lower() or "policy" in error_str.lower() or "403" in error_str:
            return None, f"STORAGE_POLICY:{error_str}"
        return None, error_str


def delete_brand_image(brand_name):
    """브랜드 이미지를 Storage에서 삭제"""
    try:
        safe_name = brand_name.replace(" ", "_").replace("/", "_")
        existing = supabase.storage.from_(BUCKET_NAME).list()
        for item in existing:
            if item.get("name", "").startswith(safe_name + "_"):
                supabase.storage.from_(BUCKET_NAME).remove([item["name"]])
        return True
    except:
        return False


# ========================
# 렌더링
# ========================

def render_brand_tab():
    """브랜드 관리 탭"""

    menu_options = ["📋 브랜드 목록"]
    if is_authenticated():
        menu_options.append("✏️ 브랜드 등록/수정")

    menu = st.radio("선택", menu_options, horizontal=True, key="brand_menu")

    st.divider()

    if menu == "📋 브랜드 목록":
        _show_brand_list()
    elif menu == "✏️ 브랜드 등록/수정":
        _show_brand_form()


def _show_brand_list():
    st.subheader("등록된 브랜드 목록")

    df = load_brands()

    if df.empty:
        st.info("등록된 브랜드가 없습니다. '브랜드 등록/수정'에서 추가해주세요.")
        return

    st.metric("등록 브랜드 수", f"{len(df)}개")
    st.divider()

    # 호버 이미지 CSS
    st.markdown("""
    <style>
    .brand-row {
        position: relative;
        display: flex;
        align-items: center;
        padding: 6px 12px;
        border-bottom: 1px solid #eee;
        gap: 16px;
    }
    .brand-row:hover {
        background: #f8f8f8;
    }
    .brand-row .brand-hover-img {
        display: none;
        position: absolute;
        top: 100%;
        left: 12px;
        z-index: 1000;
        border: 2px solid #ddd;
        border-radius: 8px;
        background: #fff;
        box-shadow: 0 4px 16px rgba(0,0,0,0.15);
        max-width: 200px;
        max-height: 200px;
        margin-top: 4px;
    }
    .brand-row:hover .brand-hover-img {
        display: block;
    }
    .brand-name {
        font-weight: 600;
        font-size: 15px;
        min-width: 120px;
    }
    .brand-desc {
        color: #666;
        font-size: 13px;
        flex: 1;
    }
    .brand-memo {
        color: #999;
        font-size: 12px;
        max-width: 200px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    </style>
    """, unsafe_allow_html=True)

    # 헤더
    st.markdown("""
    <div style="display:flex; padding:8px 12px; border-bottom:2px solid #ccc; font-weight:bold; font-size:13px; color:#555; gap:16px;">
        <div style="min-width:120px;">브랜드명</div>
        <div style="flex:1;">설명</div>
        <div style="max-width:200px;">메모</div>
    </div>
    """, unsafe_allow_html=True)

    # 브랜드 행
    for _, row in df.iterrows():
        name = row['name']
        desc = row.get("description", "") or ""
        memo = row.get("memo", "") or ""
        image_url = row.get("image_url", "") or ""

        img_tag = f'<img class="brand-hover-img" src="{image_url}" alt="{name}"/>' if image_url else ""

        st.markdown(f"""
        <div class="brand-row">
            <div class="brand-name">{name}</div>
            <div class="brand-desc">{desc}</div>
            <div class="brand-memo">{memo}</div>
            {img_tag}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 삭제
    if is_authenticated():
        st.subheader("🗑️ 브랜드 삭제")
        delete_options = df["name"].tolist()
        delete_target = st.selectbox(
            "삭제할 브랜드 선택", options=delete_options, index=None,
            placeholder="브랜드를 선택하세요...", key="brand_delete_target"
        )

        if delete_target:
            col_a, col_b = st.columns([1, 4])
            with col_a:
                if st.button("🗑️ 삭제", type="primary", key="brand_delete_btn"):
                    brand_id = df[df["name"] == delete_target]["id"].iloc[0]
                    # 이미지도 함께 삭제
                    delete_brand_image(delete_target)
                    delete_brand(brand_id)
                    st.session_state['brand_delete_success'] = f"✅ '{delete_target}' 삭제 완료"
                    st.rerun()

    # 삭제 성공 메시지
    if 'brand_delete_success' in st.session_state:
        st.success(st.session_state['brand_delete_success'])
        try:
            st.toast(st.session_state['brand_delete_success'], icon="✅")
        except:
            pass
        del st.session_state['brand_delete_success']


def _show_brand_form():
    st.subheader("브랜드 등록 / 수정")
    st.caption("이미 존재하는 브랜드명을 입력하면 자동으로 수정됩니다.")

    df = load_brands()

    # 폼 초기화
    form_reset = st.session_state.get('brand_form_reset', False)
    if form_reset:
        for key in ['brand_name_input', 'brand_desc_input', 'brand_memo_input']:
            if key in st.session_state:
                del st.session_state[key]
        st.session_state['brand_existing_select'] = 0
        del st.session_state['brand_form_reset']

    existing_options = [""] + df["name"].tolist() if not df.empty else [""]
    existing = st.selectbox(
        "기존 브랜드 수정 (새 브랜드면 비워두세요)",
        options=existing_options, index=0, key="brand_existing_select"
    )

    if form_reset:
        default_name = ""
        default_desc = ""
        default_memo = ""
        current_image_url = ""
    elif existing and not df.empty:
        row = df[df["name"] == existing].iloc[0]
        default_name = row["name"]
        default_desc = row.get("description", "") or ""
        default_memo = row.get("memo", "") or ""
        current_image_url = row.get("image_url", "") or ""
    else:
        default_name = ""
        default_desc = ""
        default_memo = ""
        current_image_url = ""

    # 성공 메시지
    if 'brand_success_msg' in st.session_state:
        st.success(st.session_state['brand_success_msg'])
        try:
            st.toast(st.session_state['brand_success_msg'], icon="✅")
        except:
            pass
        del st.session_state['brand_success_msg']
    
    # 이미지 업로드 에러 메시지
    if 'brand_image_error' in st.session_state:
        error_msg = st.session_state['brand_image_error']
        if "STORAGE_POLICY" in error_msg or "BUCKET_CREATE_FAIL" in error_msg:
            st.error("❌ 이미지 업로드에 실패했습니다. Storage 접근 정책이 필요합니다.")
            st.info(
                "💡 **Supabase SQL Editor에서 아래 SQL을 실행해주세요:**"
            )
            st.code(
                "-- brand-images 버킷에 누구나 업로드/조회/삭제 허용\n"
                "CREATE POLICY \"brand-images allow all\" ON storage.objects\n"
                "  FOR ALL USING (bucket_id = 'brand-images')\n"
                "  WITH CHECK (bucket_id = 'brand-images');",
                language="sql"
            )
        else:
            st.warning(f"⚠️ 이미지 업로드 실패: {error_msg}")
        del st.session_state['brand_image_error']

    # image_url 컬럼 안내
    has_img_col = _has_image_column()
    if not has_img_col:
        st.error("❌ brands 테이블에 image_url 컬럼이 없습니다.")
        st.info("💡 **Supabase SQL Editor에서 아래 SQL을 실행해주세요:**")
        st.code("ALTER TABLE brands ADD COLUMN IF NOT EXISTS image_url TEXT DEFAULT '';", language="sql")

    # 현재 이미지 미리보기
    if current_image_url:
        st.markdown("**현재 등록된 이미지:**")
        try:
            st.image(current_image_url, width=150)
        except:
            st.caption("🖼️ 이미지 로드 실패")

    # 이미지 업로드 (폼 밖에서 - file_uploader는 폼 안에서 제한적)
    uploaded_image = st.file_uploader(
        "🖼️ 브랜드 이미지 업로드",
        type=["png", "jpg", "jpeg", "gif", "webp"],
        key="brand_image_upload",
        help="브랜드 로고나 대표 이미지를 업로드하세요."
    )

    if uploaded_image:
        st.image(uploaded_image, width=150, caption="업로드할 이미지 미리보기")

    with st.form("brand_form"):
        name = st.text_input("브랜드명", value=default_name, placeholder="예: 한우명가, 프리미엄")
        description = st.text_input("설명", value=default_desc, placeholder="브랜드 설명...")
        memo = st.text_area("메모", value=default_memo, placeholder="추가 메모...", height=80)

        submitted = st.form_submit_button("💾 저장", type="primary")

        if submitted:
            if not name.strip():
                st.error("브랜드명을 입력해주세요.")
            else:
                try:
                    # 이미지 업로드 처리
                    image_url = current_image_url if current_image_url else None
                    image_upload_failed = False
                    if uploaded_image:
                        uploaded_image.seek(0)
                        url, error = upload_brand_image(uploaded_image, name.strip())
                        if error:
                            image_upload_failed = True
                            st.session_state['brand_image_error'] = str(error)
                        else:
                            image_url = url

                    # 이미지 없이도 브랜드는 저장
                    upsert_brand(name.strip(), description, memo, image_url)

                    if image_upload_failed:
                        st.session_state['brand_success_msg'] = f"✅ '{name}' 저장 완료! (이미지 업로드는 실패)"
                    elif existing:
                        st.session_state['brand_success_msg'] = f"✅ '{name}' 수정 완료!"
                    else:
                        st.session_state['brand_success_msg'] = f"✅ '{name}' 등록 완료!"
                    st.session_state['brand_form_reset'] = True
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ 저장 중 오류가 발생했습니다: {str(e)}")

    # 기존 이미지 삭제 버튼 (수정 모드에서만)
    if existing and current_image_url:
        st.divider()
        if st.button("🗑️ 현재 이미지 삭제", key="brand_remove_image"):
            delete_brand_image(existing)
            update_brand_image(existing, "")
            st.session_state['brand_success_msg'] = f"✅ '{existing}' 이미지 삭제 완료!"
            st.rerun()
