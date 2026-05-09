/**
 * 希望シフトのリアルタイム検証
 *
 * ソルバー側の chain ルールに基づき、入力時点で矛盾を検出する。
 * 現在のソルバーは strict chain (夜→翌日OFF / 夜→翌々日OFF) を強制しているため、
 * 2連夜勤 (夜→明→夜) と関連パターンは生成不可。
 *
 * 検出パターン:
 *   double_night       : day N=夜, day N+2=夜
 *   night_then_d2_ake  : day N=夜, day N+2=明 (chainズレ: 翌々日は休/有 のはず)
 *   ake_then_night    : day N=明, day N+1=夜 (= 夜→明→夜 の起点)
 *
 * 「夜」「管夜」は両方 NIGHT として扱い、「明」「管明」は両方 AKE として扱う。
 */

export type ConflictType = 'double_night' | 'night_then_d2_ake' | 'ake_then_night';

export interface RequestConflict {
  nurseId: number | string;
  nurseName: string;
  days: number[]; // 1-based の関連日
  type: ConflictType;
  message: string;
}

const NIGHT_LABELS = new Set(['夜', '管夜']);
const AKE_LABELS = new Set(['明', '管明']);

const isNight = (s: string | undefined | null): boolean =>
  typeof s === 'string' && NIGHT_LABELS.has(s);
const isAke = (s: string | undefined | null): boolean =>
  typeof s === 'string' && AKE_LABELS.has(s);

/**
 * ナース1人分の希望辞書 ({day: shift}) からアクセスする型。
 * day が string キー / number キーどちらでも引けるようにする。
 */
type DayMap = Record<string | number, string | undefined | null>;

const getShift = (m: DayMap | undefined, day: number): string | undefined => {
  if (!m) return undefined;
  const v = (m as any)[day] ?? (m as any)[String(day)];
  return typeof v === 'string' ? v : undefined;
};

export interface ValidateNurse {
  id: number | string;
  name: string;
}

/**
 * 希望シフトの矛盾を全ナース分まとめて返す。
 *
 * @param nurses     対象ナースリスト (id, name)
 * @param requests   {nurseId: {day: shift}} 形式 (day は 1-based)
 * @param daysInMonth 月の日数
 */
export function validateRequests(
  nurses: ReadonlyArray<ValidateNurse>,
  requests: Record<string | number, DayMap | undefined>,
  daysInMonth: number
): RequestConflict[] {
  const conflicts: RequestConflict[] = [];

  for (const nurse of nurses) {
    const nurseRequests =
      requests[nurse.id] ?? requests[String(nurse.id)] ?? undefined;
    if (!nurseRequests) continue;

    for (let d = 1; d <= daysInMonth; d++) {
      const shift = getShift(nurseRequests, d);
      if (!shift) continue;

      // パターン1: 夜 → 翌々日 = 夜 (2連夜勤)
      if (isNight(shift) && isNight(getShift(nurseRequests, d + 2))) {
        conflicts.push({
          nurseId: nurse.id,
          nurseName: nurse.name,
          days: [d, d + 1, d + 2],
          type: 'double_night',
          message: `${d}日=夜 → ${d + 2}日=夜 は2連夜勤(夜明夜)になります。間に休/有を入れてください`,
        });
      }

      // パターン2: 夜 → 翌々日 = 明 (chainズレ: 翌々日は休のはず)
      if (isNight(shift) && isAke(getShift(nurseRequests, d + 2))) {
        conflicts.push({
          nurseId: nurse.id,
          nurseName: nurse.name,
          days: [d, d + 1, d + 2],
          type: 'night_then_d2_ake',
          message: `${d}日=夜 → ${d + 2}日=明 はchainズレです（${d + 1}日が明、${d + 2}日は休/有 であるべき）`,
        });
      }

      // パターン3: 明 → 翌日 = 夜 (= 2連夜勤の起点)
      if (isAke(shift) && isNight(getShift(nurseRequests, d + 1))) {
        conflicts.push({
          nurseId: nurse.id,
          nurseName: nurse.name,
          days: [d, d + 1],
          type: 'ake_then_night',
          message: `${d}日=明 → ${d + 1}日=夜 は明の翌日に夜のため配置不可（2連夜勤の起点）`,
        });
      }
    }
  }

  return conflicts;
}

/**
 * (nurseId, day) から「矛盾しているか」を高速判定するためのヘルパー。
 * 戻り値: nurseId → Set<day> のマップ。
 */
export function buildConflictMap(
  conflicts: ReadonlyArray<RequestConflict>
): Map<string, Set<number>> {
  const map = new Map<string, Set<number>>();
  for (const c of conflicts) {
    const key = String(c.nurseId);
    let set = map.get(key);
    if (!set) {
      set = new Set<number>();
      map.set(key, set);
    }
    for (const d of c.days) set.add(d);
  }
  return map;
}
