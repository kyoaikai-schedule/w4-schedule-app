# おすすめバッジ A/B テスト: `badge_shown` カラム追加手順

## 概要

「おすすめ」バッジ自体が師長の選択に与える影響を測定する A/B テスト
（設計書 `design-badge-ab.md`）の腕を記録するカラムを
`pattern_choice_events` に追加します。変更はこの1カラムのみ。

- `true` = バッジ表示腕（システム推薦 = trueIndex 0 のパターンに表示）
- `false` = バッジ非表示腕
- `NULL` = 実験対象外（非ブラインド提示の病棟・実験開始前の行）

腕の割当は生成完了時刻の epoch 分パリティ（偶数分 = 表示）。
検算用の割当時刻と実提示の有無は `client_meta.badgeAssignedAtMs` /
`client_meta.badgeDisplayed`（JSONB 内、スキーマ変更なし）に記録されます。

## 適用手順

1. Supabase ダッシュボード → SQL Editor
2. `2026-08-22_add_badge_shown_column.sql` を貼り付けて実行

カラム追加前にアプリの新版がデプロイされても、INSERT に未知カラムが含まれると
失敗して console.warn が出るだけで、勤務表の動作には影響しません
（記録経路は fire-and-forget で完全隔離）。ただしその間の記録は失われるため、
**SQL を先に適用してからデプロイする**のが正しい順序です。

## ロールバック

```sql
ALTER TABLE public.pattern_choice_events DROP COLUMN IF EXISTS badge_shown;
```
