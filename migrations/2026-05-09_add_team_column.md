# 夜勤チーム機能 フェーズ1: マイグレーション実行手順

## 概要

`nurses` テーブル群に `team VARCHAR(10) DEFAULT NULL` カラムを追加します。
全 8 テーブル (HCU/ER/6病棟) は **同一 Supabase** に存在するため、1 回の SQL 実行で完了します。

| 対象部門 | テーブル名 |
|---|---|
| HCU | `hcu_nurses` |
| 救急外来 | `emergency_nurses` |
| 4階西病棟 | `w4_nurses` |
| 3階西病棟 | `w3_nurses` |
| 3階東病棟 | `e3_nurses` |
| 4階東病棟 | `e4_nurses` |
| 5階病棟 | `f5_nurses` |
| 外来 | `outpatient_nurses` |

## 実行手順

### 1. Supabase ダッシュボードにログイン
https://supabase.com → Project: `ercnehjywfphrgqaepzi`

### 2. SQL Editor を開く
左サイドバー「SQL Editor」→「+ New query」

### 3. 以下の SQL を貼り付けて Run

```sql
ALTER TABLE hcu_nurses        ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE emergency_nurses  ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE w4_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE w3_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE e3_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE e4_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE f5_nurses         ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
ALTER TABLE outpatient_nurses ADD COLUMN IF NOT EXISTS team VARCHAR(10) DEFAULT NULL;
```

### 4. 結果確認

```sql
SELECT table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE column_name = 'team'
  AND table_name LIKE '%_nurses'
ORDER BY table_name;
```

**期待結果: 8 行**
| table_name | column_name | data_type | is_nullable | column_default |
|---|---|---|---|---|
| e3_nurses | team | character varying | YES | NULL |
| e4_nurses | team | character varying | YES | NULL |
| emergency_nurses | team | character varying | YES | NULL |
| f5_nurses | team | character varying | YES | NULL |
| hcu_nurses | team | character varying | YES | NULL |
| outpatient_nurses | team | character varying | YES | NULL |
| w3_nurses | team | character varying | YES | NULL |
| w4_nurses | team | character varying | YES | NULL |

### 5. 動作確認

```sql
-- 既存ナースに影響がないことを確認 (team は全員 NULL)
SELECT id, name, team FROM hcu_nurses LIMIT 5;
```

## ロールバック (緊急時のみ)

```sql
ALTER TABLE hcu_nurses        DROP COLUMN IF EXISTS team;
ALTER TABLE emergency_nurses  DROP COLUMN IF EXISTS team;
ALTER TABLE w4_nurses         DROP COLUMN IF EXISTS team;
ALTER TABLE w3_nurses         DROP COLUMN IF EXISTS team;
ALTER TABLE e3_nurses         DROP COLUMN IF EXISTS team;
ALTER TABLE e4_nurses         DROP COLUMN IF EXISTS team;
ALTER TABLE f5_nurses         DROP COLUMN IF EXISTS team;
ALTER TABLE outpatient_nurses DROP COLUMN IF EXISTS team;
```

ただしフロント側で team 列に書き込みを試みた後にロールバックすると
upsert が失敗するため、フロントを旧バージョンに戻してから実行のこと。

## デプロイ順序 (重要)

1. **先に DB マイグレーションを実行**
2. 各リポを Vercel にデプロイ (新フロントは team を含む upsert を発行する)

逆順 (フロント先) にすると、ナース管理画面でチーム選択 → 保存 →
PostgreSQL がスキーマ違反でエラー、になります。
