/**
 * 日本の祝日判定（祝日法準拠）
 *
 * WardScheduleSystem と TeamScheduleTab の重複実装を1本化したもの。
 * どちらか一方だけを直して不整合になるのを防ぐため、必ずこのモジュールを使うこと。
 */

// 指定年月の第n月曜の日（1-based）。mo は 0-based。
const getNthMonday = (y: number, mo: number, n: number): number => {
  let count = 0;
  for (let d = 1; d <= 31; d++) {
    const date = new Date(y, mo, d);
    if (date.getMonth() !== mo) break;
    if (date.getDay() === 1) { count++; if (count === n) return d; }
  }
  return 1;
};

// 基礎となる祝日（振替休日・国民の休日を含まない）を年単位で列挙する。
// 戻り値は [月(1-based), 日] の配列。
const getBaseHolidays = (year: number): Array<[number, number]> => {
  const spring = Math.floor(20.8431 + 0.242194 * (year - 1980) - Math.floor((year - 1980) / 4));
  const autumn = Math.floor(23.2488 + 0.242194 * (year - 1980) - Math.floor((year - 1980) / 4));
  return [
    [1, 1],                              // 元日
    [1, getNthMonday(year, 0, 2)],       // 成人の日（1月第2月曜）
    [2, 11],                             // 建国記念の日
    [2, 23],                             // 天皇誕生日
    [3, spring],                         // 春分の日
    [4, 29],                             // 昭和の日
    [5, 3],                              // 憲法記念日
    [5, 4],                              // みどりの日
    [5, 5],                              // こどもの日
    [7, getNthMonday(year, 6, 3)],       // 海の日（7月第3月曜）
    [8, 11],                             // 山の日
    [9, getNthMonday(year, 8, 3)],       // 敬老の日（9月第3月曜）
    [9, autumn],                         // 秋分の日
    [10, getNthMonday(year, 9, 2)],      // スポーツの日（10月第2月曜）
    [11, 3],                             // 文化の日
    [11, 23],                            // 勤労感謝の日
  ];
};

// 日本の祝日を取得（年と月を指定、1-based dayの配列を返す）
// 祝日法の規定順に ①基礎祝日 → ②振替休日 → ③国民の休日 の順で計算する。
//   ②振替休日（第3条第2項）: 日曜の祝日の翌日以降で最初の「祝日でない日」。1日だけ見て終わらせない
//     （例: 2026/5/3(日)憲法記念日 → 5/4・5/5 も祝日なので 5/6 が振替休日）
//   ③国民の休日（第3条第3項）: 前日と翌日の両方が祝日である平日
//     （例: 2026/9/22(火) は 9/21 敬老の日と 9/23 秋分の日に挟まれるため休日）
// 月末月初・年末年始の判定で前後の月/年を参照する必要があるため、
// 前後の年も含めて計算し、最後に対象月で絞り込む。
export const getJapaneseHolidays = (year: number, month: number): number[] => {
  // month は 0-based (0=1月, 11=12月)
  const keyOf = (dt: Date) => `${dt.getFullYear()}-${dt.getMonth() + 1}-${dt.getDate()}`;
  const years = [year - 1, year, year + 1];

  // ① 基礎祝日
  const base = new Set<string>();
  years.forEach(y => getBaseHolidays(y).forEach(([m, d]) => base.add(`${y}-${m}-${d}`)));

  // ② 振替休日: 日曜の基礎祝日について、翌日から順に見て最初の「基礎祝日でない日」
  const substitutes = new Set<string>();
  years.forEach(y => getBaseHolidays(y).forEach(([m, d]) => {
    const dt = new Date(y, m - 1, d);
    if (dt.getDay() !== 0) return;
    const next = new Date(y, m - 1, d + 1);
    while (base.has(keyOf(next))) next.setDate(next.getDate() + 1);
    substitutes.add(keyOf(next));
  }));

  const isHoliday = (dt: Date) => base.has(keyOf(dt)) || substitutes.has(keyOf(dt));

  // ③ 国民の休日: 前日と翌日の両方が祝日（基礎祝日 or 振替休日）である平日
  //    土日は対象外（曜日で別に扱われるため祝日として扱う必要がない）
  const daysInM = new Date(year, month + 1, 0).getDate();
  const result: number[] = [];
  for (let d = 1; d <= daysInM; d++) {
    const dt = new Date(year, month, d);
    if (isHoliday(dt)) { result.push(d); continue; }
    const dow = dt.getDay();
    if (dow === 0 || dow === 6) continue;
    if (isHoliday(new Date(year, month, d - 1)) && isHoliday(new Date(year, month, d + 1))) result.push(d);
  }
  return result;
};
