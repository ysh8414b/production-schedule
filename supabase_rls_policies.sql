-- ============================================
-- Supabase RLS (Row Level Security) 정책 설정
-- Supabase 대시보드 > SQL Editor 에서 실행하세요
-- ============================================
-- 모든 테이블: 누구나 읽기 가능, 인증된 사용자만 쓰기 가능

-- 기존 정책 제거 (이미 있을 경우 충돌 방지)
DO $$
DECLARE
    tbl TEXT;
    pol RECORD;
BEGIN
    FOR tbl IN
        SELECT unnest(ARRAY[
            'products', 'sales', 'schedules', 'uploaded_products',
            'raw_meats', 'raw_meat_inputs', 'product_rawmeats',
            'loss_assignments', 'production_status_uploads',
            'production_status_groups', 'production_status_items',
            'brands', 'losses', 'production_records', 'loading_products'
        ])
    LOOP
        FOR pol IN
            SELECT policyname FROM pg_policies WHERE tablename = tbl
        LOOP
            EXECUTE format('DROP POLICY IF EXISTS %I ON %I', pol.policyname, tbl);
        END LOOP;
    END LOOP;
END $$;

-- ============================================
-- products
-- ============================================
ALTER TABLE products ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON products FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON products FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON products FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON products FOR DELETE TO authenticated USING (true);

-- ============================================
-- sales
-- ============================================
ALTER TABLE sales ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON sales FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON sales FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON sales FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON sales FOR DELETE TO authenticated USING (true);

-- ============================================
-- schedules
-- ============================================
ALTER TABLE schedules ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON schedules FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON schedules FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON schedules FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON schedules FOR DELETE TO authenticated USING (true);

-- ============================================
-- uploaded_products
-- ============================================
ALTER TABLE uploaded_products ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON uploaded_products FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON uploaded_products FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON uploaded_products FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON uploaded_products FOR DELETE TO authenticated USING (true);

-- ============================================
-- raw_meats
-- ============================================
ALTER TABLE raw_meats ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON raw_meats FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON raw_meats FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON raw_meats FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON raw_meats FOR DELETE TO authenticated USING (true);

-- ============================================
-- raw_meat_inputs
-- ============================================
ALTER TABLE raw_meat_inputs ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON raw_meat_inputs FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON raw_meat_inputs FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON raw_meat_inputs FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON raw_meat_inputs FOR DELETE TO authenticated USING (true);

-- ============================================
-- product_rawmeats
-- ============================================
ALTER TABLE product_rawmeats ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON product_rawmeats FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON product_rawmeats FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON product_rawmeats FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON product_rawmeats FOR DELETE TO authenticated USING (true);

-- ============================================
-- loss_assignments
-- ============================================
ALTER TABLE loss_assignments ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON loss_assignments FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON loss_assignments FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON loss_assignments FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON loss_assignments FOR DELETE TO authenticated USING (true);

-- ============================================
-- production_status_uploads
-- ============================================
ALTER TABLE production_status_uploads ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON production_status_uploads FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON production_status_uploads FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON production_status_uploads FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON production_status_uploads FOR DELETE TO authenticated USING (true);

-- ============================================
-- production_status_groups
-- ============================================
ALTER TABLE production_status_groups ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON production_status_groups FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON production_status_groups FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON production_status_groups FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON production_status_groups FOR DELETE TO authenticated USING (true);

-- ============================================
-- production_status_items
-- ============================================
ALTER TABLE production_status_items ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON production_status_items FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON production_status_items FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON production_status_items FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON production_status_items FOR DELETE TO authenticated USING (true);

-- ============================================
-- brands
-- ============================================
ALTER TABLE brands ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON brands FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON brands FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON brands FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON brands FOR DELETE TO authenticated USING (true);

-- ============================================
-- losses
-- ============================================
ALTER TABLE losses ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON losses FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON losses FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON losses FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON losses FOR DELETE TO authenticated USING (true);

-- ============================================
-- production_records
-- ============================================
ALTER TABLE production_records ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON production_records FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON production_records FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON production_records FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON production_records FOR DELETE TO authenticated USING (true);

-- ============================================
-- loading_products
-- ============================================
ALTER TABLE loading_products ENABLE ROW LEVEL SECURITY;
CREATE POLICY "read_all" ON loading_products FOR SELECT USING (true);
CREATE POLICY "insert_auth" ON loading_products FOR INSERT TO authenticated WITH CHECK (true);
CREATE POLICY "update_auth" ON loading_products FOR UPDATE TO authenticated USING (true) WITH CHECK (true);
CREATE POLICY "delete_auth" ON loading_products FOR DELETE TO authenticated USING (true);
