# 選好学習 Stage 1: `pattern_choice_events` テーブル追加手順

## 概要

勤務表自動生成の3パターン提示時に、各パターンの特徴量と「どれが採用されたか」
（またはキャンセル）を記録するテーブルを追加します。Stage 3 の選好モデル
（Bradley-Terry / ロジスティック回帰）の学習データになります。
設計書: `design-preference-stage1.md`（2026-08-22 承認）。

`schedule_drafts` と同方式の **全病棟共有・prefix なし** テーブルで、
`ward` カラムでアプリ間を区別します。既存テーブルへの変更はありません。

| カラム | 型 | 説明 |
|---|---|---|
| `id` | UUID PK | 内部 ID |
| `event_id` | UUID UNIQUE | フロント採番。二重クリックでも行が重複しない |
| `created_at` | TIMESTAMPTZ | 記録日時 |
| `ward` | TEXT | `'w4'` など（dbPrefix と同一系） |
| `target_year` / `target_month` | INT | 対象年月。**month は 1-12**（`schedule_drafts` の 0-11 とは異なるので注意） |
| `generation_mode` | TEXT | `'solver'` / `'local'` |
| `feature_version` | INT | 特徴量定義の版（現在 1） |
| `blinded` | BOOLEAN | ブラインドUI（案1/2/3・おすすめ非表示）で提示されたか |
| `patterns` | JSONB | 3パターン分の特徴量・真の同一性・表示位置 |
| `outcome` | TEXT | `'adopted'` / `'cancelled'` |
| `adopted_true_index` / `adopted_display_pos` | INT | 採用パターンの真のindex / 画面位置（cancelled は NULL） |
| `client_meta` | JSONB | 日数・看護師数などの文脈 |

## 適用手順

1. Supabase ダッシュボード → SQL Editor
2. `2026-08-22_add_pattern_choice_events.sql` の内容を貼り付けて実行
3. Table Editor で `pattern_choice_events` が作成されたことを確認

## ロールバック

```sql
DROP TABLE IF EXISTS public.pattern_choice_events;
```

（共有テーブルのため、他病棟展開後は全病棟の記録が消える点に注意）

## 安全性

- アプリからは INSERT のみ（RLS で INSERT のみ許可）。失敗しても
  `console.warn` を出すだけで、勤務表の生成・保存・表示には影響しない
  （`src/lib/preferenceLog.ts` 内で完全に隔離）。
