-- おすすめバッジ A/B テスト（設計書 design-badge-ab.md）: 腕カラム追加のみ。
-- pattern_choice_events 以外の既存テーブル・カラムには一切触れない。

ALTER TABLE public.pattern_choice_events
  ADD COLUMN IF NOT EXISTS badge_shown BOOLEAN;

COMMENT ON COLUMN public.pattern_choice_events.badge_shown IS
  'A/B腕: true=おすすめバッジ表示 / false=非表示 / NULL=実験対象外(非ブラインド提示・実験開始前の行)';
