// 休暇台帳（フェーズ1）: 勤務表の記号から休暇種別の計上を導出する純関数。
// 設計書: design-leave-types.md（2026-08-27 承認、決定事項3点反映）
//
// 会計規則（provenance ルール・設計書 §1）:
//   休           → 公休 1.0
//   午前半/午後半 → 公休 0.5（半休）
//   有           → 年休 1.0
//   ﾈ/ ・ /ﾈ     → 年休 0.5（半日年休。ﾈ は半角カタカナ U+FF88）
//   その他 off カテゴリ（欠/産/育/忌/結/介/特/生理 等） → どちらにも不算入
//   明・管明     → 勤務扱い（プロジェクト規約。休暇ではない）
//
// 台帳は保存しない。常に schedule の記号から計算する（二重管理をしない）。

export interface LeaveCounts {
  kokyuFull: number;    // 休 の日数
  hankyuHalves: number; // 午前半+午後半 の個数（1個 = 0.5公休）
  nenkyuFull: number;   // 有 の日数
  nenkyuHalves: number; // ﾈ/ + /ﾈ の個数（1個 = 0.5年休）
  otherOff: number;     // 公休/年休に算入しない休暇（off カテゴリのその他記号）
  kokyuCount: number;   // 公休カウント = 休 + 0.5×半休（Excel の規定公休カウントと同じ意味論）
  nenkyuCount: number;  // 年休カウント = 有 + 0.5×半日年休
}

const KOKYU_FULL = new Set(['休']);
const HANKYU_HALF = new Set(['午前半', '午後半']);
const NENKYU_FULL = new Set(['有']);
const NENKYU_HALF = new Set(['ﾈ/', '/ﾈ']);

// isOtherOff: off カテゴリのカスタム記号か（呼び出し側が allShifts の category から判定を渡す）
export const countLeave = (
  shifts: (string | null | undefined)[],
  isOtherOff: (symbol: string) => boolean
): LeaveCounts => {
  let kokyuFull = 0, hankyuHalves = 0, nenkyuFull = 0, nenkyuHalves = 0, otherOff = 0;
  shifts.forEach(s => {
    if (!s || typeof s !== 'string') return;
    if (KOKYU_FULL.has(s)) kokyuFull++;
    else if (HANKYU_HALF.has(s)) hankyuHalves++;
    else if (NENKYU_FULL.has(s)) nenkyuFull++;
    else if (NENKYU_HALF.has(s)) nenkyuHalves++;
    else if (isOtherOff(s)) otherOff++;
  });
  return {
    kokyuFull, hankyuHalves, nenkyuFull, nenkyuHalves, otherOff,
    kokyuCount: kokyuFull + 0.5 * hankyuHalves,
    nenkyuCount: nenkyuFull + 0.5 * nenkyuHalves,
  };
};

// 0.5単位の数値を表示用に整形（10 → "10", 10.5 → "10.5"）
export const fmtHalf = (v: number): string => (Number.isInteger(v) ? String(v) : v.toFixed(1));
