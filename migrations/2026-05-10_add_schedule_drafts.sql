-- =====================================================================
-- Migration: schedule_drafts テーブル追加 (フェーズ4: 下書き保存・管理)
-- 日付: 2026-05-10
-- 対象: 全部署 (1テーブル / ward カラムで区別)
-- =====================================================================
--
-- 仕様:
--   - 全 7 リポ (hcu, emergency, w4, w3, e3, e4, f5, outpatient) で共有
--   - ward カラムでアプリ間を区別
--   - schedule_data: 既存 schedules テーブルと同じ構造の JSONB
--   - team_metrics: /solve_team が返す teamMetrics をそのまま JSONB で保存
--   - source: 'team' (チーム編成タブで生成) | 'normal' (将来用)
--   - 既存 *_schedules テーブルは一切触らない (独立)
-- =====================================================================

CREATE TABLE IF NOT EXISTS schedule_drafts (
  id SERIAL PRIMARY KEY,
  ward VARCHAR(20) NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  schedule_data JSONB NOT NULL,
  team_metrics JSONB,
  source VARCHAR(20) DEFAULT 'team',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_schedule_drafts_ward_year_month
  ON schedule_drafts(ward, year, month);

-- 確認クエリ:
--   SELECT column_name, data_type, is_nullable, column_default
--   FROM information_schema.columns
--   WHERE table_name = 'schedule_drafts'
--   ORDER BY ordinal_position;
--
-- 期待結果: 9行 (id/ward/year/month/name/schedule_data/team_metrics/source/created_at/updated_at)
