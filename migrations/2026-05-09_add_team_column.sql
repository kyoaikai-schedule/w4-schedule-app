-- =====================================================================
-- Migration: nurses テーブルに team カラム追加 (フェーズ1: 基盤のみ)
-- 日付: 2026-05-09
-- 対象: HCU (hcu_*) / 救急外来 (emergency_*) / 6病棟 (w4/w3/e3/e4/f5/outpatient)
-- =====================================================================
--
-- 仕様:
--   - 'A'〜'E' を想定 (将来拡張余地のため VARCHAR(10))
--   - NULL 許容 (既存ナースは未設定のまま)
--   - DEFAULT NULL (新規追加時もチーム未設定)
--   - 既存自動生成は team を一切参照しないため影響なし
--   - フェーズ2でソルバーが team を参照する別エンドポイントが追加予定
--
-- 全テーブル(8個)で同一カラムを追加する。すでに存在する場合は IF NOT EXISTS
-- でスキップ。
-- =====================================================================

ALTER TABLE hcu_nurses        ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE emergency_nurses  ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE w4_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE w3_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE e3_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE e4_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE f5_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE outpatient_nurses ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;

-- 確認クエリ (実行後にカラムが存在することを確認):
--
--   SELECT table_name, column_name, data_type, is_nullable, column_default
--   FROM information_schema.columns
--   WHERE column_name = 'team'
--     AND table_name LIKE '%_nurses'
--   ORDER BY table_name;
--
-- 期待結果: 8行 (各 *_nurses テーブルが返る、data_type = character varying,
--          is_nullable = YES, column_default = NULL)
