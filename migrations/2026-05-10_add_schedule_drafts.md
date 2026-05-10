# フェーズ4: 下書き保存テーブル `schedule_drafts` 追加手順

## 概要

夜勤チーム編成タブで生成した結果を「下書き」として複数保存できるよう、
専用テーブルを追加します。全 7 リポ (HCU + 6 病棟) は同一 Supabase の
**1 テーブル** を共有し、`ward` カラムでアプリ間を区別します。

| カラム | 型 | 説明 |
|---|---|---|
| `id` | SERIAL PK | 内部 ID |
| `ward` | VARCHAR(20) | `'hcu'` / `'emergency'` / `'w4'` / `'w3'` / `'e3'` / `'e4'` / `'f5'` / `'outpatient'` |
| `year` | INT | 対象年 |
| `month` | INT | 対象月 (0〜11, JS 形式) |
| `name` | VARCHAR(100) | ユーザーが付ける名前 (例: `ベテラン重視案`) |
| `schedule_data` | JSONB | `{nurseId: shifts[]}` (既存 schedules と同形式) |
| `team_metrics` | JSONB | `/solve_team` 由来の teamMetrics |
| `source` | VARCHAR(20) | `'team'` (チーム編成生成) / 将来用に `'normal'` |
| `created_at` | TIMESTAMPTZ | 作成日時 |
| `updated_at` | TIMESTAMPTZ | 更新日時 |

## 実行手順

1. https://supabase.com にログイン → Project `ercnehjywfphrgqaepzi`
2. 左サイドバー **SQL Editor → + New query**
3. 以下の SQL を貼り付けて **Run**:

```sql
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
```

4. 確認:

```sql
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_name = 'schedule_drafts'
ORDER BY ordinal_position;
```

期待: 10 行 (id/ward/year/month/name/schedule_data/team_metrics/source/created_at/updated_at)

## ロールバック (緊急時のみ)

```sql
DROP INDEX IF EXISTS idx_schedule_drafts_ward_year_month;
DROP TABLE IF EXISTS schedule_drafts;
```

## デプロイ順序

1. **先に DB マイグレーション実行**
2. 各リポ (hcu + 6 ward) を Vercel にデプロイ
3. 順序を逆にすると、新フロントが下書き保存を試みた時に "table not found"
   でエラーになります。

## 既存テーブルへの影響

なし。`*_schedules` / `*_nurses` / `*_requests` 等の既存テーブルは
一切変更していません。下書きは独立した `schedule_drafts` の 1 つに
集約されます (採用時のみ既存 `*_schedules` に書き戻す)。
