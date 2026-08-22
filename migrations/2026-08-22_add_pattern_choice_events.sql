-- 選好学習 Stage 1: 採用パターン記録テーブル（設計書 design-preference-stage1.md §2）
-- 全 7 病棟共有・prefix なし（schedule_drafts と同方式、ward カラムで区別）。
-- 既存テーブルへの変更は一切なし。アプリからは INSERT のみ（SELECT しない）。
-- 注意: target_month は 1-12（人間可読）。schedule_drafts の month (0-11, JS形式) とは異なる。

CREATE TABLE IF NOT EXISTS public.pattern_choice_events (
  id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id            UUID NOT NULL UNIQUE,        -- フロントで採番。二重クリック/再送の重複防止
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  ward                TEXT NOT NULL,               -- 'w4' / 'hcu' / 'emergency' / 'outpatient' / ...
  target_year         INT  NOT NULL,
  target_month        INT  NOT NULL,               -- 1-12
  generation_mode     TEXT NOT NULL CHECK (generation_mode IN ('solver', 'local')),
  feature_version     INT  NOT NULL DEFAULT 1,     -- 特徴量定義の版
  blinded             BOOLEAN NOT NULL DEFAULT false, -- ランダム化+ブラインドUIで提示されたか
  patterns            JSONB NOT NULL,              -- 3要素の配列: {true_index, true_label, display_pos, has_error, score, relax_level, fallback_mode, features}
  outcome             TEXT NOT NULL CHECK (outcome IN ('adopted', 'cancelled')),
  adopted_true_index  INT,                         -- 0..2（ソルバー返却順=真の同一性）。cancelled は NULL
  adopted_display_pos INT,                         -- 0..2（画面上の位置）。cancelled は NULL
  client_meta         JSONB,                       -- {daysInMonth, nurseCount} 等の再現用文脈
  CHECK ((outcome = 'adopted') = (adopted_true_index IS NOT NULL AND adopted_display_pos IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS pce_ward_month_idx
  ON public.pattern_choice_events (ward, target_year, target_month);

-- RLS: 既存テーブルは Allow all だが、本テーブルは INSERT のみ許可（アプリが SELECT しないため最小権限）。
-- 分析は Supabase ダッシュボード / service role（RLS バイパス）から行う。
ALTER TABLE public.pattern_choice_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow insert to pattern_choice_events"
  ON public.pattern_choice_events FOR INSERT WITH CHECK (true);
