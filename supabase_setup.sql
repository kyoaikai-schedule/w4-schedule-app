-- ============================================
-- 勤務表管理システム Supabase テーブル作成SQL
-- ============================================
-- このSQLをSupabaseダッシュボードの「SQL Editor」で実行してください

-- 1. 職員マスタ
CREATE TABLE IF NOT EXISTS nurses (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  position TEXT NOT NULL DEFAULT '一般',
  department TEXT NOT NULL DEFAULT 'HCU',
  active BOOLEAN DEFAULT true,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 2. 休み希望
CREATE TABLE IF NOT EXISTS requests (
  id SERIAL PRIMARY KEY,
  nurse_id INTEGER NOT NULL REFERENCES nurses(id) ON DELETE CASCADE,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL,
  shift_type TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(nurse_id, year, month, day)
);

-- 3. 勤務表
CREATE TABLE IF NOT EXISTS schedules (
  id SERIAL PRIMARY KEY,
  nurse_id INTEGER NOT NULL REFERENCES nurses(id) ON DELETE CASCADE,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day INTEGER NOT NULL,
  shift TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(nurse_id, year, month, day)
);

-- 4. 前月制約
CREATE TABLE IF NOT EXISTS prev_month_constraints (
  id SERIAL PRIMARY KEY,
  nurse_id INTEGER NOT NULL REFERENCES nurses(id) ON DELETE CASCADE,
  year INTEGER NOT NULL,
  month INTEGER NOT NULL,
  day_index INTEGER NOT NULL,
  shift TEXT NOT NULL,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
  UNIQUE(nurse_id, year, month, day_index)
);

-- 5. 設定
CREATE TABLE IF NOT EXISTS settings (
  id SERIAL PRIMARY KEY,
  key TEXT NOT NULL UNIQUE,
  value TEXT,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================
-- Row Level Security (RLS) - 全テーブルで公開アクセスを許可
-- （認証なしでアクセスできるようにする）
-- ============================================

ALTER TABLE nurses ENABLE ROW LEVEL SECURITY;
ALTER TABLE requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE schedules ENABLE ROW LEVEL SECURITY;
ALTER TABLE prev_month_constraints ENABLE ROW LEVEL SECURITY;
ALTER TABLE settings ENABLE ROW LEVEL SECURITY;

-- 全テーブルに読み書き許可ポリシーを作成
CREATE POLICY "Allow all access to nurses" ON nurses FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to requests" ON requests FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to schedules" ON schedules FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to prev_month_constraints" ON prev_month_constraints FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Allow all access to settings" ON settings FOR ALL USING (true) WITH CHECK (true);

-- ============================================
-- インデックス（パフォーマンス改善）
-- ============================================

CREATE INDEX IF NOT EXISTS idx_nurses_department ON nurses(department);
CREATE INDEX IF NOT EXISTS idx_requests_year_month ON requests(year, month);
CREATE INDEX IF NOT EXISTS idx_schedules_year_month ON schedules(year, month);
CREATE INDEX IF NOT EXISTS idx_prev_constraints_year_month ON prev_month_constraints(year, month);

-- ============================================
-- マイグレーション: 部門対応（既存DBに対して実行）
-- ============================================
-- 既存のnursesテーブルにdepartmentカラムを追加
-- ALTER TABLE nurses ADD COLUMN department TEXT NOT NULL DEFAULT 'HCU';
-- CREATE INDEX idx_nurses_department ON nurses(department);

-- settingsテーブルの既存キーを部門プレフィックス付きにリネーム
-- UPDATE settings SET key = 'nurseShiftPrefs-HCU' WHERE key = 'nurseShiftPrefs';
-- UPDATE settings SET key = REPLACE(key, 'prevMonth-', 'prevMonth-HCU-')
--   WHERE key LIKE 'prevMonth-%' AND key NOT LIKE 'prevMonth-HCU-%';

-- ============================================
-- マイグレーション: 職員表示順（display_order）
-- ============================================
ALTER TABLE hcu_nurses ADD COLUMN IF NOT EXISTS display_order INTEGER DEFAULT 0;
ALTER TABLE emergency_nurses ADD COLUMN IF NOT EXISTS display_order INTEGER DEFAULT 0;
