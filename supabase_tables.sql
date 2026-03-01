-- ============================================
-- 제품, 로스관리 전면수정 - 테이블 생성 SQL
-- Supabase SQL Editor에서 실행하세요
-- ============================================

-- 1. uploaded_products 테이블 (제품 업로드용)
CREATE TABLE IF NOT EXISTS uploaded_products (
  id bigint generated always as identity primary key,
  product_code text NOT NULL UNIQUE,
  product_name text NOT NULL,
  origin text DEFAULT '',
  kg_per_box numeric DEFAULT 0,
  production_time_per_unit integer DEFAULT 0,
  production_point text DEFAULT '주야',
  minimum_production_quantity integer DEFAULT 0,
  current_stock integer DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- 2. production_status_uploads 테이블 (업로드 배치)
CREATE TABLE IF NOT EXISTS production_status_uploads (
  id bigint generated always as identity primary key,
  upload_date date NOT NULL,
  file_name text DEFAULT '',
  total_groups integer DEFAULT 0,
  total_input_kg numeric DEFAULT 0,
  total_output_kg numeric DEFAULT 0,
  total_loss_kg numeric DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

-- 3. production_status_groups 테이블 (그룹별 로스)
CREATE TABLE IF NOT EXISTS production_status_groups (
  id bigint generated always as identity primary key,
  upload_id bigint REFERENCES production_status_uploads(id) ON DELETE CASCADE,
  group_index integer NOT NULL,
  total_input_kg numeric DEFAULT 0,
  total_output_kg numeric DEFAULT 0,
  loss_kg numeric DEFAULT 0,
  loss_rate numeric DEFAULT 0,
  total_input_amount numeric DEFAULT 0,
  total_output_amount numeric DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

-- 4. production_status_items 테이블 (행별 원본 데이터)
CREATE TABLE IF NOT EXISTS production_status_items (
  id bigint generated always as identity primary key,
  group_id bigint REFERENCES production_status_groups(id) ON DELETE CASCADE,
  item_type text NOT NULL,
  meat_code text DEFAULT '',
  meat_name text DEFAULT '',
  meat_origin text DEFAULT '',
  meat_grade text DEFAULT '',
  meat_boxes numeric DEFAULT 0,
  meat_kg numeric DEFAULT 0,
  meat_unit text DEFAULT '',
  meat_amount numeric DEFAULT 0,
  product_code text DEFAULT '',
  product_name text DEFAULT '',
  product_origin text DEFAULT '',
  product_grade text DEFAULT '',
  product_boxes numeric DEFAULT 0,
  product_kg numeric DEFAULT 0,
  product_unit text DEFAULT '',
  product_amount numeric DEFAULT 0,
  expected_sales_amount numeric DEFAULT 0,
  expected_profit_amount numeric DEFAULT 0,
  created_at timestamptz DEFAULT now()
);

-- RLS 정책 (기존 테이블과 동일한 패턴)
ALTER TABLE uploaded_products ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_status_uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_status_groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_status_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow all for anon" ON uploaded_products FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON production_status_uploads FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON production_status_groups FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all for anon" ON production_status_items FOR ALL USING (true) WITH CHECK (true);
