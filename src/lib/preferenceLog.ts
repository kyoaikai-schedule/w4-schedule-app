// 選好学習 Stage 1: 採用パターン記録モジュール
// 設計書: design-preference-stage1.md（2026-08-22 承認）
//
// 絶対制約: このモジュールの失敗が勤務表の生成・保存・表示に影響してはならない。
// - preparePatternsForSelect は例外時に入力配列をそのまま返す（提示は現行どおり）
// - recordPatternChoice は fire-and-forget。呼び出し側は await しない。
//   全経路 try/catch で、失敗は console.warn のみ。

import { supabase } from './supabase';

export const PREF_LOG_BLINDED = true; // w4 パイロット: フルブラインド提示（案1/2/3、順序シャッフル）
export const PREF_AB_BADGE = true; // おすすめバッジ A/B テスト（design-badge-ab.md）。ブラインド提示時のみ有効
const FEATURE_VERSION = 1;

// バッジ A/B の腕割当: 生成完了時刻の epoch 分パリティ（偶数分 = バッジ表示）。
// preparePatternsForSelect で1回だけ評価して _prefEvent に固定するため、
// モーダル再レンダリングで腕が変わる（判断中にバッジが出没する）ことは構造的にない。
const assignBadgeArm = (nowMs: number): boolean => Math.floor(nowMs / 60000) % 2 === 0;
const TABLE = 'pattern_choice_events'; // 全病棟共有・prefix なし（schedule_drafts と同方式）

export interface PrefFeatureCtx {
  daysInMonth: number;
  weekendHolidayDays: number[]; // 0-based の土日祝インデックス
  maxConsec: number;            // 連続勤務上限（超過日数を f8 で違反カウント）
  nurseIds: (number | string)[]; // 生成対象看護師（excludeFromGeneration 除外後）
  requests: Record<string, Record<string, string>>; // nurseId → 日(1-based) → 希望ラベル
  dayReqByDay: number[];   // 0-based 日次の日勤必要人数（buildDailyRequirements で構築）
  nightReqByDay: number[]; // 0-based 日次の夜勤必要人数（同上）
}

// f9/f10 用の日次必要人数テーブルを1つの定義で構築する。
// solver の metrics は年末年始特例を知らず、local の metrics は夜勤実績に管夜を含める
// など経路間で定義が食い違うため、metrics を使わずここで統一定義を持つ。
// ロジックは WardScheduleSystem.tsx の getDayStaffReq / getWeeklyNightStaff / getNightReq と同一。
export const buildDailyRequirements = (opts: {
  daysInMonth: number;
  targetYear: number;
  targetMonth: number; // 0-based (JS Date 形式。getDayStaffReq と同じ基準)
  weekendHolidayDays: number[];
  generateConfig: any; // weekdayDayStaff / weekendDayStaff / yearEndDayStaff / newYearDayStaff / nightShiftPattern / startWithThree
}): { dayReqByDay: number[]; nightReqByDay: number[] } => {
  const { daysInMonth, targetYear, targetMonth, weekendHolidayDays, generateConfig: gc } = opts;
  const num = (v: any, fb: number) => (Number.isFinite(Number(v)) ? Number(v) : fb);
  const weekday = num(gc?.weekdayDayStaff, 6);
  const weekend = num(gc?.weekendDayStaff, 5);
  const yearEnd = num(gc?.yearEndDayStaff, weekend);
  const newYear = num(gc?.newYearDayStaff, weekend);
  const wkndSet = new Set(weekendHolidayDays);

  const dayReqByDay: number[] = [];
  for (let d = 0; d < daysInMonth; d++) {
    const isYearEnd = targetMonth === 11 && (d + 1 === 30 || d + 1 === 31);
    const isNewYear = targetMonth === 0 && d + 1 >= 1 && d + 1 <= 3;
    dayReqByDay.push(isYearEnd ? yearEnd : isNewYear ? newYear : wkndSet.has(d) ? weekend : weekday);
  }

  // 週ごとの夜勤人数（getWeeklyNightStaff と同一ロジック）
  const np = Array.isArray(gc?.nightShiftPattern) ? gc.nightShiftPattern : [4, 4];
  const c0 = num(np[0], 4);
  const c1 = num(np[1], 4);
  const startWithThree = !!gc?.startWithThree;
  const weeks: { s: number; e: number; c: number }[] = [];
  const firstDow = new Date(targetYear, targetMonth, 1).getDay();
  let cur = 1, wi = 0;
  const dUS = firstDow === 0 ? 0 : (7 - firstDow);
  if (dUS > 0) {
    weeks.push({ s: 1, e: Math.min(dUS, daysInMonth), c: startWithThree ? c0 : c1 });
    cur = dUS + 1; wi = 1;
  }
  while (cur <= daysInMonth) {
    const pi = startWithThree ? (wi % 2) : ((wi + 1) % 2);
    const ed = Math.min(cur + 6, daysInMonth);
    weeks.push({ s: cur, e: ed, c: pi === 0 ? c0 : c1 });
    cur = ed + 1; wi++;
  }
  const nightReqByDay: number[] = [];
  for (let d = 0; d < daysInMonth; d++) {
    const w = weeks.find(p => d + 1 >= p.s && d + 1 <= p.e);
    nightReqByDay.push(w ? w.c : 3); // 3 は getNightReq のフォールバックと同じ
  }
  return { dayReqByDay, nightReqByDay };
};

export interface PrefEventMeta {
  ward: string;
  targetYear: number;
  targetMonth: number; // 1-12（呼び出し側で JS 0-based から +1 して渡す）
  generationMode: 'solver' | 'local';
}

// シフト判定の統一定義（設計書 §3）:
// 休日 = 休/有 のみ（明・管明は勤務扱い）。夜勤回数カウントは「夜」のみ。
// 連勤ランは 明/管明 でリセット（既存 scoreFn / consecViolations と同じ扱い）。
const isOff = (s: any) => s === '休' || s === '有';
const isAke = (s: any) => s === '明' || s === '管明';
const isWork = (s: any) => !!s && !isOff(s) && !isAke(s);

const round1 = (v: number) => Math.round(v * 10) / 10;
const spread = (xs: number[]) => (xs.length ? Math.max(...xs) - Math.min(...xs) : 0);

// 全特徴量を pat.data から統一再計算する（solver/local で metrics の定義が異なるため）。
// 唯一の例外は f7(relaxLevel): ソルバー内部状態なので再計算不能、metrics から転記。
// f9/f10 の実績夜勤カウントは f3 と同じく「夜」のみ（管夜は含めない。solver metrics と同基準、
// local metrics は管夜を含むため使わない）。日勤実績は「日」のみ（両経路とも同じ）。
export const computeFeatures = (
  data: Record<string, (string | null)[]>,
  metrics: any,
  ctx: PrefFeatureCtx
) => {
  const rows = ctx.nurseIds
    .map(id => data[String(id)])
    .filter((sh): sh is (string | null)[] => Array.isArray(sh));

  const offCounts = rows.map(sh => sh.filter(isOff).length);
  const nightCounts = rows.map(sh => sh.filter(s => s === '夜').length);
  // 土日祝の勤務日数（明も勤務扱い = 休/有/空以外すべて）
  const wkndCounts = rows.map(sh =>
    ctx.weekendHolidayDays.filter(d => { const s = sh[d]; return !!s && !isOff(s); }).length
  );

  const offMean = offCounts.length ? offCounts.reduce((a, b) => a + b, 0) / offCounts.length : 0;

  let consec3Runs = 0;      // f5: 長さ>=3 の極大連勤ラン数（合法だが疲労する3連勤の多さ）
  let consecViolations = 0; // f8用: 上限(maxConsec)超過の延べ日数
  let nullCells = 0;        // f8用: 空セル数
  rows.forEach(sh => {
    let run = 0;
    const closeRun = () => { if (run >= 3) consec3Runs++; run = 0; };
    sh.forEach(s => {
      if (isWork(s)) { run++; if (run > ctx.maxConsec) consecViolations++; }
      else closeRun();
      if (s === null || s === '') nullCells++;
    });
    closeRun();
  });

  // 日次不足（f9/f10）: 統一必要人数テーブルとの差分
  let dayShortage = 0, nightShortage = 0;
  for (let d = 0; d < ctx.daysInMonth; d++) {
    let dc = 0, nc = 0;
    rows.forEach(sh => { if (sh[d] === '日') dc++; if (sh[d] === '夜') nc++; });
    dayShortage += Math.max(0, (ctx.dayReqByDay[d] ?? 0) - dc);
    nightShortage += Math.max(0, (ctx.nightReqByDay[d] ?? 0) - nc);
  }

  // 希望一致率（研/出張等はモーダル表示前に原ラベルへ復元済みのため単純比較でよい）
  let reqTotal = 0, reqMatched = 0;
  const idSet = new Set(ctx.nurseIds.map(String));
  Object.entries(ctx.requests || {}).forEach(([nid, days]) => {
    if (!idSet.has(String(nid))) return;
    const sh = data[String(nid)];
    if (!Array.isArray(sh)) return;
    Object.entries(days || {}).forEach(([day, label]) => {
      const d = Number(day) - 1;
      if (typeof label !== 'string' || !label || !(d >= 0 && d < ctx.daysInMonth)) return;
      reqTotal++;
      if (sh[d] === label) reqMatched++;
    });
  });

  return {
    f1_off_dev_sum: round1(offCounts.reduce((a, b) => a + Math.abs(b - offMean), 0)),
    f2_off_spread: spread(offCounts),
    f3_night_spread: spread(nightCounts),
    f4_req_match: reqTotal > 0 ? round1((reqMatched / reqTotal) * 100) : 100,
    f5_consec3_runs: consec3Runs,
    f6_weekend_spread: spread(wkndCounts),
    f7_relax_level: typeof metrics?.relaxLevel === 'number' ? metrics.relaxLevel : null,
    f8_violations: consecViolations + nullCells,
    f9_day_shortage: dayShortage,
    f10_night_shortage: nightShortage,
  };
};

// パターン受信直後（setGeneratedPatterns の直前）に呼ぶ:
// 特徴量計算 → シャッフル → 表示位置/イベントID付与。
// 失敗したら入力をそのまま返す（提示・採用は現行どおり動き、記録だけ諦める）。
export const preparePatternsForSelect = (
  patterns: any[],
  ctx: PrefFeatureCtx,
  meta: PrefEventMeta
): any[] => {
  try {
    if (typeof crypto === 'undefined' || typeof crypto.randomUUID !== 'function') return patterns;
    const eventId = crypto.randomUUID();
    const badgeAssignedAtMs = Date.now();
    // 腕はイベント単位でここで確定（null = 実験対象外: 非ブラインド提示）
    const badgeShown: boolean | null =
      PREF_LOG_BLINDED && PREF_AB_BADGE ? assignBadgeArm(badgeAssignedAtMs) : null;
    const clientMeta = { daysInMonth: ctx.daysInMonth, nurseCount: ctx.nurseIds.length, badgeAssignedAtMs };

    const withPref = patterns.map((p, i) => {
      const hasError = !p.data || Object.keys(p.data).length === 0 || !!p.metrics?.error;
      let features: any = null;
      try { features = computeFeatures(p.data || {}, p.metrics || {}, ctx); }
      catch (e) { console.warn('[prefLog] computeFeatures 失敗:', e); }
      return {
        ...p,
        _prefEvent: { eventId, clientMeta, badgeShown, ...meta, blinded: PREF_LOG_BLINDED },
        _pref: {
          trueIndex: i,
          trueLabel: p.label ?? null,
          hasError,
          score: typeof p.score === 'number' ? p.score : null,
          relaxLevel: typeof p.metrics?.relaxLevel === 'number' ? p.metrics.relaxLevel : null,
          fallbackMode: p.metrics?.fallbackMode ?? null,
          features,
          displayPos: i,
        },
      };
    });

    const shuffled = [...withPref];
    if (PREF_LOG_BLINDED) {
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
      }
    }
    shuffled.forEach((p, pos) => { p._pref = { ...p._pref, displayPos: pos }; });
    return shuffled;
  } catch (e) {
    console.warn('[prefLog] preparePatternsForSelect 失敗（記録なしで続行）:', e);
    return patterns;
  }
};

// 採用（adopted = 採用したパターン）またはキャンセル（adopted = null）を記録する。
// fire-and-forget: 戻り値 void。呼び出し側の処理を一切ブロック・失敗させない。
// event_id は unique + ignoreDuplicates なので二重クリックでも行は重複しない。
export const recordPatternChoice = (patterns: any[], adopted: any | null): void => {
  try {
    const ev = patterns?.[0]?._prefEvent;
    if (!ev?.eventId) return; // prepare が失敗していた場合は静かに何もしない

    const adoptedTrueIndex = adopted?._pref?.trueIndex;
    const adoptedDisplayPos = adopted?._pref?.displayPos;
    // 採用なのに位置情報が欠けている場合は DB の CHECK 制約に反するため記録しない
    if (adopted && (typeof adoptedTrueIndex !== 'number' || typeof adoptedDisplayPos !== 'number')) return;

    const row = {
      event_id: ev.eventId,
      ward: ev.ward,
      target_year: ev.targetYear,
      target_month: ev.targetMonth,
      generation_mode: ev.generationMode,
      feature_version: FEATURE_VERSION,
      blinded: !!ev.blinded,
      patterns: [...patterns]
        .sort((a, b) => (a?._pref?.trueIndex ?? 0) - (b?._pref?.trueIndex ?? 0))
        .map(p => ({
          true_index: p?._pref?.trueIndex ?? null,
          true_label: p?._pref?.trueLabel ?? null,
          display_pos: p?._pref?.displayPos ?? null,
          has_error: p?._pref?.hasError ?? null,
          score: p?._pref?.score ?? null,
          relax_level: p?._pref?.relaxLevel ?? null,
          fallback_mode: p?._pref?.fallbackMode ?? null,
          features: p?._pref?.features ?? null,
        })),
      outcome: adopted ? 'adopted' : 'cancelled',
      adopted_true_index: adopted ? adoptedTrueIndex : null,
      adopted_display_pos: adopted ? adoptedDisplayPos : null,
      badge_shown: typeof ev.badgeShown === 'boolean' ? ev.badgeShown : null, // A/B腕（null = 実験対象外）
      client_meta: {
        ...(ev.clientMeta ?? {}),
        // 実提示: badge_shown=true でも trueIndex 0 が解なしならバッジは出ない（ITT と実提示を区別）
        badgeDisplayed: typeof ev.badgeShown === 'boolean'
          ? (ev.badgeShown && patterns.some(p => p?._pref?.trueIndex === 0 && !p?._pref?.hasError))
          : null,
      },
    };

    Promise.resolve(
      supabase.from(TABLE).upsert(row as any, { onConflict: 'event_id', ignoreDuplicates: true })
    )
      .then(({ error }: any) => { if (error) console.warn('[prefLog] insert 失敗:', error.message); })
      .catch((e: any) => console.warn('[prefLog] insert 例外:', e));
  } catch (e) {
    console.warn('[prefLog] recordPatternChoice 失敗:', e);
  }
};
