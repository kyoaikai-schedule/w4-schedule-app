/**
 * 夜勤チーム編成タブ (フェーズ3 + フェーズ4)
 *
 * フェーズ2 の /solve_team エンドポイントを呼び出し、teamMetrics を可視化する。
 * フェーズ4 で「下書き保存・管理」機能を追加 (schedule_drafts テーブル使用)。
 *
 * 既存の自動生成タブ・既存ロジックには一切干渉しない (本コンポーネント単体で完結)。
 */
import { useEffect, useMemo, useState } from 'react';
import { Users, Sparkles, X, RefreshCw, Save, FolderOpen, Trash2, Star, ChevronDown, ChevronRight } from 'lucide-react';
import { supabase } from '../lib/supabase';

interface NurseLite {
  id: number | string;
  name: string;
  team?: string | null;
}

interface PerDayBalance {
  day: number;
  expected: string[];
  actual: string[];
  isBalanced: boolean;
  missing: string[];
  extra: string[];
}

interface ImprovementSuggestion {
  priority: number;
  team: string;
  type: 'increase_max_night_shifts' | 'enable_night_shift' | 'transfer_nurse' | string;
  title: string;
  description: string;
  targetNurses?: string[];
  currentCapacity?: number;
  expectedCapacity?: number;
  expectedDemand?: number;
  feasibility: 'easy' | 'medium' | 'hard' | string;
  fromTeam?: string;
  toTeam?: string;
}

interface TeamMetrics {
  teamMode?: boolean;
  teamCount?: number;
  usedTeams?: string[];
  fallbackLevel?: number;
  attemptsTeam?: Array<{ relaxTeam: number; status: string; elapsedSec: number }>;
  perDayTeamBalance?: PerDayBalance[];
  balanceRate?: number;
  balancedDays?: number;
  totalDays?: number;
  diagnostics?: {
    teamMode?: boolean;
    requiredTeams?: string[];
    nursesWithTeam?: number;
    nursesWithoutTeam?: number;
    perTeamCount?: Record<string, number>;
    warnings?: string[];
  };
  feasibility?: {
    isFullyFeasible?: boolean;
    currentMaxRate?: number;
    diagnosis?: string;
    perTeamInfo?: Record<string, { count: number; capacity: number; demand: number }>;
  };
  improvementSuggestions?: ImprovementSuggestion[];
}

interface TeamPattern {
  label: string;
  data: Record<string, string[]>;
  score: number;
  metrics?: { teamMetrics?: TeamMetrics } & Record<string, unknown>;
}

interface GenerateConfigLite {
  nightShiftPattern: number[];
  startWithThree?: boolean;
  weekdayDayStaff: number;
  weekendDayStaff: number;
  yearEndDayStaff?: number;
  newYearDayStaff?: number;
  [key: string]: unknown;
}

interface Props {
  show: boolean;
  onClose: () => void;
  buildRequest: () => unknown;
  solverAPIUrl: string;
  solverAPIKey: string;
  activeNurses: NurseLite[];
  onAcceptPattern: (data: Record<string, string[]>) => void;
  // フェーズ4 追加: schedule_drafts へ書き込むのに必要
  ward: string;
  targetYear: number;
  targetMonth: number;
  // 集計行 (tfoot) の要件判定に使用 (通常モードの勤務表と同じ表示にするため)
  generateConfig: GenerateConfigLite;
}

// ──────────────────────────────────────
// HcuScheduleSystem の同名関数と同一ロジック (集計行で使用)
// ──────────────────────────────────────
const getDayOfWeekJa = (year: number, month: number, day: number): string => {
  const d = new Date(year, month, day);
  return ['日', '月', '火', '水', '木', '金', '土'][d.getDay()];
};

const getJapaneseHolidaysJa = (year: number, month: number): number[] => {
  const holidays: number[] = [];
  const m = month + 1;
  if (m === 1) { holidays.push(1); holidays.push(11); }
  if (m === 2) holidays.push(23);
  if (m === 3) holidays.push(21);
  if (m === 4) holidays.push(29);
  if (m === 5) { holidays.push(3); holidays.push(4); holidays.push(5); }
  if (m === 7) holidays.push(20);
  if (m === 8) holidays.push(11);
  if (m === 9) { holidays.push(16); holidays.push(23); }
  if (m === 10) holidays.push(14);
  if (m === 11) { holidays.push(3); holidays.push(23); }
  const getNthMonday = (y: number, mo: number, n: number): number => {
    let count = 0;
    for (let d = 1; d <= 31; d++) {
      const date = new Date(y, mo, d);
      if (date.getMonth() !== mo) break;
      if (date.getDay() === 1) { count++; if (count === n) return d; }
    }
    return -1;
  };
  if (m === 1) { const d = getNthMonday(year, month, 2); if (d > 0) holidays.push(d); }
  if (m === 7) { const d = getNthMonday(year, month, 3); if (d > 0) holidays.push(d); }
  if (m === 9) { const d = getNthMonday(year, month, 3); if (d > 0) holidays.push(d); }
  if (m === 10) { const d = getNthMonday(year, month, 2); if (d > 0) holidays.push(d); }
  return Array.from(new Set(holidays)).sort((a, b) => a - b);
};

// 指定日 (i: 0-based) の夜勤必要数。HcuScheduleSystem の tfoot と同一ロジック。
const getNightRequiredAt = (
  i: number,
  daysInMonth: number,
  targetYear: number,
  targetMonth: number,
  cfg: GenerateConfigLite,
): number => {
  const firstDow = new Date(targetYear, targetMonth, 1).getDay();
  const weeks: { s: number; e: number; c: number }[] = [];
  let cur = 1, wi = 0;
  const dUS = firstDow === 0 ? 0 : (7 - firstDow);
  const pattern = cfg.nightShiftPattern || [4, 4];
  if (dUS > 0) {
    weeks.push({ s: 1, e: Math.min(dUS, daysInMonth), c: cfg.startWithThree ? pattern[0] : pattern[1] });
    cur = dUS + 1; wi = 1;
  }
  while (cur <= daysInMonth) {
    const pi = cfg.startWithThree ? (wi % 2) : ((wi + 1) % 2);
    const ed = Math.min(cur + 6, daysInMonth);
    weeks.push({ s: cur, e: ed, c: pattern[pi] });
    cur = ed + 1; wi++;
  }
  const d = i + 1;
  for (const p of weeks) { if (d >= p.s && d <= p.e) return p.c; }
  return 3;
};

// 指定日 (i: 0-based) の日勤必要数と「厳格判定」(weekend/holiday/year-end/new-year なら厳格)。
const getDayShiftRequirementAt = (
  i: number,
  targetYear: number,
  targetMonth: number,
  cfg: GenerateConfigLite,
  holidays: number[],
): { minRequired: number; isStrict: boolean } => {
  const dow = getDayOfWeekJa(targetYear, targetMonth, i + 1);
  const isWeekend = dow === '土' || dow === '日';
  const day = i + 1;
  const isYearEnd = targetMonth === 11 && (day === 30 || day === 31);
  const isNewYear = targetMonth === 0 && (day >= 1 && day <= 3);
  const isNatHol = holidays.includes(day);
  const minRequired = isYearEnd ? (cfg.yearEndDayStaff ?? cfg.weekendDayStaff) :
                      isNewYear ? (cfg.newYearDayStaff ?? cfg.weekendDayStaff) :
                      (isWeekend || isNatHol) ? cfg.weekendDayStaff :
                      cfg.weekdayDayStaff;
  const isStrict = isWeekend || isNatHol || isYearEnd || isNewYear;
  return { minRequired, isStrict };
};

// 通常モードと同じ tfoot (集計行 5行) を返すレンダラ
function renderScheduleTfoot(
  scheduleData: Record<string, string[]>,
  activeNurses: NurseLite[],
  daysInMonth: number,
  targetYear: number,
  targetMonth: number,
  cfg: GenerateConfigLite,
): JSX.Element {
  const holidays = getJapaneseHolidaysJa(targetYear, targetMonth);
  return (
    <tfoot className="sticky bottom-0 z-20">
      {/* 夜勤人数 (= '夜' のみカウント。'管夜' は除外) */}
      <tr className="bg-purple-50 font-bold">
        <td className="border px-2 py-1 sticky left-0 bg-purple-50 z-30 text-purple-800">夜勤人数</td>
        {Array.from({ length: daysInMonth }, (_, i) => {
          let count = 0;
          activeNurses.forEach(nurse => {
            const shift = (scheduleData[String(nurse.id)] || [])[i];
            if (shift === '夜') count++;
          });
          const nightRequired = getNightRequiredAt(i, daysInMonth, targetYear, targetMonth, cfg);
          return (
            <td
              key={i}
              className={`border text-center p-1 text-purple-700 min-w-[28px] ${
                count < nightRequired ? 'bg-red-200 text-red-700' :
                count > nightRequired ? 'bg-yellow-200 text-yellow-700' : ''
              }`}
            >
              <div>{count}</div>
              <div className="text-[9px] text-gray-400">/{nightRequired}</div>
            </td>
          );
        })}
      </tr>
      {/* 夜明人数 (= '明' のみカウント。'管明' は除外) */}
      <tr className="bg-pink-50 font-bold">
        <td className="border px-2 py-1 sticky left-0 bg-pink-50 z-30 text-pink-800">夜明人数</td>
        {Array.from({ length: daysInMonth }, (_, i) => {
          let count = 0;
          activeNurses.forEach(nurse => {
            const shift = (scheduleData[String(nurse.id)] || [])[i];
            if (shift === '明') count++;
          });
          return (
            <td key={i} className="border text-center p-1 text-pink-700 min-w-[28px]">{count}</td>
          );
        })}
      </tr>
      {/* 日勤人数 (= '日' のみカウント) */}
      <tr className="bg-blue-50 font-bold">
        <td className="border px-2 py-1 sticky left-0 bg-blue-50 z-30 text-blue-800">日勤人数</td>
        {Array.from({ length: daysInMonth }, (_, i) => {
          let count = 0;
          activeNurses.forEach(nurse => {
            const shift = (scheduleData[String(nurse.id)] || [])[i];
            if (shift === '日') count++;
          });
          const { minRequired, isStrict } = getDayShiftRequirementAt(i, targetYear, targetMonth, cfg, holidays);
          const isDeviation = isStrict
            ? count !== minRequired
            : (count < minRequired || count > minRequired + 2);
          return (
            <td
              key={i}
              className={`border text-center p-1 text-blue-700 min-w-[28px] ${
                isDeviation ? 'outline outline-3 outline-red-500 -outline-offset-1 bg-red-50' : ''
              }`}
            >
              <div>{count}</div>
              <div className="text-[9px] text-gray-400">/{isStrict ? minRequired : `${minRequired}-${minRequired + 2}`}</div>
            </td>
          );
        })}
      </tr>
      {/* 休日人数 (= '休' or '有' をカウント。半休は 0.5) */}
      <tr className="bg-gray-100 font-bold">
        <td className="border px-2 py-1 sticky left-0 bg-gray-100 z-30 text-gray-700">休日人数</td>
        {Array.from({ length: daysInMonth }, (_, i) => {
          let count = 0;
          activeNurses.forEach(nurse => {
            const shift = (scheduleData[String(nurse.id)] || [])[i];
            if (shift === '休' || shift === '有') count++;
            else if (shift === '午前半' || shift === '午後半') count += 0.5;
          });
          return (
            <td key={i} className="border text-center p-1 text-gray-600 min-w-[28px]">{count}</td>
          );
        })}
      </tr>
      {/* 出勤計 (= 休/有/明/管明/半休 以外の有効ラベル: 日/夜/管夜) */}
      <tr className="bg-amber-50 font-bold">
        <td className="border px-2 py-1 sticky left-0 bg-amber-50 z-30 text-amber-800">出勤計</td>
        {Array.from({ length: daysInMonth }, (_, i) => {
          let count = 0;
          activeNurses.forEach(nurse => {
            const shift = (scheduleData[String(nurse.id)] || [])[i];
            if (shift && shift !== '休' && shift !== '有' && shift !== '明' && shift !== '管明' && shift !== '午前半' && shift !== '午後半') count++;
          });
          return (
            <td key={i} className="border text-center p-1 text-amber-700 min-w-[28px]">{count}</td>
          );
        })}
      </tr>
    </tfoot>
  );
}

interface DraftRow {
  id: number;
  ward: string;
  year: number;
  month: number;
  name: string;
  schedule_data: Record<string, string[]>;
  team_metrics: TeamMetrics | null;
  source: string;
  created_at: string;
  updated_at: string;
}

const TEAM_BG_COLORS: Record<string, string> = {
  A: 'bg-blue-100 text-blue-900 border-blue-300',
  B: 'bg-green-100 text-green-900 border-green-300',
  C: 'bg-yellow-100 text-yellow-900 border-yellow-300',
  D: 'bg-pink-100 text-pink-900 border-pink-300',
  E: 'bg-purple-100 text-purple-900 border-purple-300',
};

const fallbackLabel = (level: number | undefined): string => {
  switch (level) {
    case 0: return '完全配分 (チーム制約厳格)';
    case 1: return '一部許容 (チーム制約緩め)';
    case 2: return '制約解除 (チーム未考慮)';
    case -1: return '解なし';
    default: return '不明';
  }
};

const feasibilityLabel = (f: string | undefined): string => {
  if (f === 'easy') return '簡単';
  if (f === 'medium') return '普通';
  if (f === 'hard') return '要相談';
  return f ?? '不明';
};

/** 100% 達成不可能時の改善提案を表示するパネル */
function ImprovementSuggestionsPanel({ teamMetrics }: { teamMetrics?: TeamMetrics | null }) {
  const sugs = teamMetrics?.improvementSuggestions ?? [];
  const feasibility = teamMetrics?.feasibility;
  if (!sugs || sugs.length === 0) return null;
  const currentMaxRate = feasibility?.currentMaxRate;
  const currentMaxPct = typeof currentMaxRate === 'number' ? (currentMaxRate * 100).toFixed(0) : null;
  return (
    <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mt-3 mb-4">
      <h4 className="font-bold text-blue-900 mb-2 flex items-center gap-2">
        💡 100% 達成のための改善提案
      </h4>
      <p className="text-sm text-blue-800 mb-3">
        {currentMaxPct != null
          ? <>現在の数学的上限: <strong>{currentMaxPct}%</strong>。以下のいずれかの設定変更で改善できます:</>
          : <>以下のいずれかの設定変更で改善できる可能性があります:</>}
      </p>
      <ol className="space-y-3">
        {sugs.map((s, i) => (
          <li key={i} className="flex gap-2">
            <span className="font-bold text-blue-700 shrink-0">{String.fromCharCode(65 + i)}.</span>
            <div className="min-w-0 flex-1">
              <p className="font-semibold text-gray-900">{s.title}</p>
              <p className="text-sm text-gray-700 break-words">{s.description}</p>
              {s.targetNurses && s.targetNurses.length > 0 && (
                <p className="text-xs text-gray-600 mt-1">
                  対象: {s.targetNurses.join('、')}
                </p>
              )}
              {typeof s.expectedCapacity === 'number' && typeof s.currentCapacity === 'number' && (
                <p className="text-xs text-gray-600 mt-0.5">
                  容量: {s.currentCapacity} → <strong>{s.expectedCapacity}</strong>
                  {typeof s.expectedDemand === 'number' && (
                    <span className="text-gray-500"> (必要 {s.expectedDemand}、{s.expectedCapacity >= s.expectedDemand ? '✅ 達成' : '⚠️ 不足'})</span>
                  )}
                </p>
              )}
              <p className="text-xs text-blue-600 mt-1">
                実施容易さ: {feasibilityLabel(s.feasibility)}
              </p>
            </div>
          </li>
        ))}
      </ol>
      <p className="text-xs text-gray-500 mt-3">
        ※ 設定変更後、ナース管理画面で対応するナースを編集してから再生成してください。
      </p>
    </div>
  );
}

export default function TeamScheduleTab({
  show,
  onClose,
  buildRequest,
  solverAPIUrl,
  solverAPIKey,
  activeNurses,
  onAcceptPattern,
  ward,
  targetYear,
  targetMonth,
  generateConfig,
}: Props) {
  // 月の日数 (集計行 tfoot で使用)
  const daysInMonth = new Date(targetYear, targetMonth + 1, 0).getDate();
  const [teamPatterns, setTeamPatterns] = useState<TeamPattern[]>([]);
  const [showTeamDetail, setShowTeamDetail] = useState<boolean[]>([]);
  const [showUnassignedDetail, setShowUnassignedDetail] = useState(false);
  const [loading, setLoading] = useState(false);
  const [generatingPhase, setGeneratingPhase] = useState('');

  // フェーズ4: 下書き機能
  const [view, setView] = useState<'generate' | 'drafts'>('generate');
  const [drafts, setDrafts] = useState<DraftRow[]>([]);
  const [draftsLoading, setDraftsLoading] = useState(false);
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [draftName, setDraftName] = useState('');
  const [pendingPatternIndex, setPendingPatternIndex] = useState<number | null>(null);
  const [expandedDraftIds, setExpandedDraftIds] = useState<Set<number>>(new Set());
  const toggleDraft = (id: number) => {
    setExpandedDraftIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const fetchDrafts = async () => {
    setDraftsLoading(true);
    try {
      const { data, error } = await supabase
        .from('schedule_drafts')
        .select('*')
        .eq('ward', ward)
        .eq('year', targetYear)
        .eq('month', targetMonth)
        .order('created_at', { ascending: false });
      if (error) throw error;
      setDrafts((data as DraftRow[]) || []);
    } catch (e: any) {
      console.error('[fetchDrafts] error:', e);
      // 'relation "schedule_drafts" does not exist' は無視 (マイグレーション未実行)
      if (!String(e?.message || '').includes('schedule_drafts')) {
        alert(`下書き読み込みエラー: ${e?.message ?? '不明'}`);
      }
      setDrafts([]);
    } finally {
      setDraftsLoading(false);
    }
  };

  useEffect(() => {
    if (show) fetchDrafts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [show, ward, targetYear, targetMonth]);

  const nursesWithoutTeam = useMemo(
    () => activeNurses.filter(n => !n.team),
    [activeNurses]
  );

  // 可視化: 現在 state にロード済みの team 別人数 (フロントから API に送られる値)
  const teamStatus = useMemo(() => {
    const counts: Record<string, number> = { A: 0, B: 0, C: 0, D: 0, E: 0 };
    let withTeam = 0;
    let withoutTeam = 0;
    activeNurses.forEach(n => {
      const t = (n.team as string) || null;
      if (t) {
        counts[t] = (counts[t] || 0) + 1;
        withTeam++;
      } else {
        withoutTeam++;
      }
    });
    return { counts, withTeam, withoutTeam, total: activeNurses.length };
  }, [activeNurses]);

  const teamOfNurse = useMemo(() => {
    const map: Record<string, string | null> = {};
    activeNurses.forEach(n => { map[String(n.id)] = (n.team as string) || null; });
    return map;
  }, [activeNurses]);

  if (!show) return null;

  const generateTeamSchedule = async () => {
    setLoading(true);
    setGeneratingPhase('AI最適化サーバーに接続中...');
    setTeamPatterns([]);
    setShowTeamDetail([]);
    try {
      const reqBody = buildRequest();
      // DEBUG: 送信内容を可視化 (フェーズ3問題切り分け用)
      console.log('[solve_team request] body.config', (reqBody as any)?.config);
      console.log('[solve_team request] body.nurses with team', (reqBody as any)?.nurses);

      // 防御: 送信される nurses の team フィールドを実機で検証
      const reqNurses: any[] = ((reqBody as any)?.nurses) || [];
      const teamSentCount = reqNurses.filter(n => n && n.team).length;
      const teamFieldExists = reqNurses.length > 0 && 'team' in reqNurses[0];
      console.log(`[solve_team request] team field exists in payload: ${teamFieldExists}, ` +
                  `nurses with team set: ${teamSentCount}/${reqNurses.length}`);
      if (!teamFieldExists) {
        alert(
          'リクエスト payload に team フィールドが含まれていません。\n' +
          '原因: フロントエンドが古いバージョンの可能性。\n' +
          '対処: ハードリロード (Cmd+Shift+R) してください。'
        );
        setLoading(false);
        setGeneratingPhase('');
        return;
      }
      if (teamSentCount === 0 && teamStatus.withTeam > 0) {
        // state には team があるのに payload に乗っていない異常
        alert(
          `フロントの state にはチーム ${teamStatus.withTeam}名 ありますが、\n` +
          'リクエスト payload には全員 team=null になっています。\n' +
          'ハードリロード (Cmd+Shift+R) で再試行してください。'
        );
        setLoading(false);
        setGeneratingPhase('');
        return;
      }

      setGeneratingPhase('AI最適化を実行中...');
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 300000);

      const response = await fetch(`${solverAPIUrl}/solve_team`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': solverAPIKey,
        },
        body: JSON.stringify(reqBody),
        signal: controller.signal,
      });
      clearTimeout(timeout);

      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      const patterns: TeamPattern[] = data.patterns || [];
      setTeamPatterns(patterns);
      setShowTeamDetail(patterns.map(() => false));
    } catch (e: any) {
      console.error('[solve_team] error:', e);
      alert(`生成エラー: ${e?.message ?? '不明'}`);
    } finally {
      setLoading(false);
      setGeneratingPhase('');
    }
  };

  const acceptPattern = (idx: number) => {
    const pattern = teamPatterns[idx];
    if (!pattern || !pattern.data || Object.keys(pattern.data).length === 0) {
      alert('解なしのため採用できません');
      return;
    }
    const balanceRate = pattern.metrics?.teamMetrics?.balanceRate ?? 0;
    const ratePct = (balanceRate * 100).toFixed(0);
    const confirmed = confirm(
      `${pattern.label} を正式採用しますか?\n\n` +
      `⚠️ 現在の勤務表は上書きされます。\n` +
      `この操作は取り消せません。\n\n` +
      `バランス達成率: ${ratePct}%\n\n` +
      `※ 後で比較したい場合は、先に「下書きに保存」してください。`
    );
    if (!confirmed) return;
    onAcceptPattern(pattern.data as Record<string, string[]>);
    alert('正式採用しました。既存の自動生成タブで確認できます。');
  };

  // === フェーズ4: 下書き保存 ===
  const openSaveDialog = (idx: number) => {
    const pattern = teamPatterns[idx];
    if (!pattern || !pattern.data || Object.keys(pattern.data).length === 0) {
      alert('解なしのため保存できません');
      return;
    }
    const ratePct = ((pattern.metrics?.teamMetrics?.balanceRate ?? 0) * 100).toFixed(0);
    setPendingPatternIndex(idx);
    setDraftName(`${pattern.label} (バランス${ratePct}%)`);
    setShowSaveDialog(true);
  };

  const saveDraft = async () => {
    if (pendingPatternIndex === null) return;
    const pattern = teamPatterns[pendingPatternIndex];
    if (!pattern) return;
    const name = draftName.trim();
    if (!name) {
      alert('下書き名を入力してください');
      return;
    }
    try {
      const { error } = await supabase.from('schedule_drafts').insert({
        ward,
        year: targetYear,
        month: targetMonth,
        name,
        schedule_data: pattern.data,
        team_metrics: pattern.metrics?.teamMetrics ?? null,
        source: 'team',
      });
      if (error) throw error;
      alert(`下書き「${name}」を保存しました`);
      setShowSaveDialog(false);
      setPendingPatternIndex(null);
      setDraftName('');
      fetchDrafts();
    } catch (e: any) {
      console.error('[saveDraft] error:', e);
      const msg = String(e?.message || '');
      if (msg.includes('schedule_drafts') && msg.includes('does not exist')) {
        alert('下書きテーブルが見つかりません。\nDB マイグレーション (2026-05-10_add_schedule_drafts.sql) を実行してください。');
      } else {
        alert(`下書き保存エラー: ${e?.message ?? '不明'}`);
      }
    }
  };

  const promoteDraftToSchedule = (draft: DraftRow) => {
    const balanceRate = draft.team_metrics?.balanceRate ?? 0;
    const ratePct = (balanceRate * 100).toFixed(0);
    const confirmed = confirm(
      `下書き「${draft.name}」を正式採用しますか?\n\n` +
      `⚠️ 現在の勤務表は上書きされます。\n` +
      `この操作は取り消せません。\n\n` +
      `バランス達成率: ${ratePct}%`
    );
    if (!confirmed) return;
    onAcceptPattern(draft.schedule_data);
    alert(`「${draft.name}」を正式採用しました。`);
  };

  const deleteDraft = async (draft: DraftRow) => {
    if (!confirm(`下書き「${draft.name}」を削除しますか?`)) return;
    try {
      const { error } = await supabase.from('schedule_drafts').delete().eq('id', draft.id);
      if (error) throw error;
      fetchDrafts();
    } catch (e: any) {
      console.error('[deleteDraft] error:', e);
      alert(`削除エラー: ${e?.message ?? '不明'}`);
    }
  };

  const formatDate = (iso: string) => {
    try {
      const d = new Date(iso);
      return `${d.getFullYear()}/${d.getMonth() + 1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2, '0')}`;
    } catch { return iso; }
  };

  return (
    <div className="fixed inset-0 bg-black/50 z-50 overflow-y-auto">
      <div className="min-h-full flex items-start justify-center p-4">
        <div className="bg-white rounded-2xl p-6 w-full max-w-7xl my-4">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-bold flex items-center gap-2">
              <Users size={22} /> 夜勤チーム編成
            </h2>
            <button onClick={onClose} className="p-2 hover:bg-gray-100 rounded-full">
              <X size={20} />
            </button>
          </div>

          <p className="text-sm text-gray-600 mb-4">
            ナース管理画面で設定したチーム (A〜E) を考慮して夜勤を割当てます。
            各日の夜勤にチームから 1 名ずつ配置することを目指し、達成困難な場合は段階的に
            制約を緩和します。生成は <code>/solve_team</code> エンドポイントを使用。
          </p>

          {/* チーム読み込み状況パネル (フロント state に何が乗っているかを可視化) */}
          <div className={`rounded-xl p-3 mb-3 border ${
            teamStatus.withTeam === 0
              ? 'bg-red-50 border-red-300'
              : 'bg-indigo-50 border-indigo-200'
          }`}>
            <div className="flex flex-wrap items-center gap-3 text-sm">
              <span className="font-bold text-gray-700">チーム読込状況:</span>
              <span>所属あり <strong className={teamStatus.withTeam === 0 ? 'text-red-600' : 'text-indigo-700'}>{teamStatus.withTeam}</strong>名</span>
              <span className="text-gray-500">/</span>
              <span>所属なし <strong>{teamStatus.withoutTeam}</strong>名</span>
              <span className="text-gray-500">/</span>
              <span>全 {teamStatus.total}名</span>
              <span className="text-gray-300">|</span>
              {(['A', 'B', 'C', 'D', 'E'] as const).map(t => (
                <span
                  key={t}
                  className={`text-xs px-2 py-0.5 rounded ${TEAM_BG_COLORS[t]} ${
                    teamStatus.counts[t] === 0 ? 'opacity-40' : ''
                  }`}
                >
                  {t}: {teamStatus.counts[t]}
                </span>
              ))}
            </div>
            {teamStatus.withTeam === 0 && (
              <p className="mt-2 text-xs text-red-700">
                ⚠️ 全員チーム未設定。ナース管理画面で設定後、ハードリロード (Cmd+Shift+R)
                で再ロードしてください。Vercel デプロイ反映遅延の場合 1〜2 分待ってから再試行。
              </p>
            )}
          </div>

          {/* ビュー切替トグル (生成 / 下書き一覧) */}
          <div className="flex gap-2 mb-4 border-b border-gray-200">
            <button
              onClick={() => setView('generate')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                view === 'generate'
                  ? 'border-indigo-600 text-indigo-700'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <Sparkles size={14} className="inline mr-1" />
              生成
            </button>
            <button
              onClick={() => setView('drafts')}
              className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
                view === 'drafts'
                  ? 'border-indigo-600 text-indigo-700'
                  : 'border-transparent text-gray-500 hover:text-gray-700'
              }`}
            >
              <FolderOpen size={14} className="inline mr-1" />
              下書き一覧 ({drafts.length}件)
            </button>
          </div>

          {view === 'drafts' && (
            <div>
              <div className="flex justify-between items-center mb-3">
                <h3 className="font-bold text-lg">
                  {targetYear}年{targetMonth + 1}月の下書き ({drafts.length}件)
                </h3>
                <button
                  onClick={fetchDrafts}
                  disabled={draftsLoading}
                  className="text-xs px-2 py-1 bg-gray-100 hover:bg-gray-200 rounded disabled:opacity-50"
                >
                  {draftsLoading ? '読込中...' : '再読込'}
                </button>
              </div>
              {drafts.length === 0 ? (
                <p className="text-gray-500 text-sm py-8 text-center">
                  下書きはまだありません。「生成」タブでパターンを作成し、
                  「下書きに保存」ボタンから保存できます。
                </p>
              ) : (
                <div className="space-y-2">
                  {drafts.map(draft => {
                    const tm = draft.team_metrics ?? {};
                    const ratePct = ((tm.balanceRate ?? 0) * 100).toFixed(0);
                    const isExpanded = expandedDraftIds.has(draft.id);
                    return (
                      <div key={draft.id} className="border-2 rounded-xl p-3 bg-white">
                        <div
                          className="flex justify-between items-start gap-3 cursor-pointer hover:bg-gray-50 -m-3 p-3 rounded-xl"
                          onClick={() => toggleDraft(draft.id)}
                        >
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-1">
                              {isExpanded
                                ? <ChevronDown size={18} className="text-gray-500 shrink-0" />
                                : <ChevronRight size={18} className="text-gray-500 shrink-0" />}
                              <h4 className="font-bold truncate">{draft.name}</h4>
                            </div>
                            <p className="text-xs text-gray-500 ml-6">
                              作成: {formatDate(draft.created_at)}
                              {draft.source && ` (${draft.source})`}
                            </p>
                            <div className="mt-1 ml-6 flex items-center gap-3 text-sm flex-wrap">
                              <span>
                                バランス達成率 <strong>{ratePct}%</strong>
                                {tm.balancedDays != null && tm.totalDays != null && (
                                  <span className="text-gray-500"> ({tm.balancedDays}/{tm.totalDays}日)</span>
                                )}
                              </span>
                              {(tm.usedTeams || []).map(t => (
                                <span
                                  key={t}
                                  className={`text-[10px] px-1.5 py-0.5 rounded ${TEAM_BG_COLORS[t] ?? 'bg-gray-100'}`}
                                >{t}</span>
                              ))}
                            </div>
                          </div>
                          <div className="flex gap-1 shrink-0" onClick={(e) => e.stopPropagation()}>
                            <button
                              onClick={() => promoteDraftToSchedule(draft)}
                              className="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded text-xs font-bold flex items-center gap-1"
                              title="正式採用 (既存勤務表に上書き)"
                            >
                              <Star size={14} /> 正式採用
                            </button>
                            <button
                              onClick={() => deleteDraft(draft)}
                              className="p-2 text-red-600 hover:bg-red-50 rounded"
                              title="削除"
                            >
                              <Trash2 size={16} />
                            </button>
                          </div>
                        </div>
                        {isExpanded && (
                          <>
                            <ImprovementSuggestionsPanel teamMetrics={draft.team_metrics} />
                            <div className="mt-3 overflow-auto max-h-72 border rounded">
                            <table className="text-xs border-collapse">
                              <thead className="bg-gray-100 sticky top-0 z-20">
                                <tr>
                                  <th className="border px-2 py-1 sticky left-0 bg-gray-100 z-30">氏名</th>
                                  {Object.values(draft.schedule_data)[0]?.map((_, dIdx) => (
                                    <th key={dIdx} className="border px-1 py-1 min-w-[28px]">{dIdx + 1}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {activeNurses.map(nurse => {
                                  const shifts = draft.schedule_data[String(nurse.id)] || [];
                                  const team = teamOfNurse[String(nurse.id)];
                                  return (
                                    <tr key={nurse.id}>
                                      <td className="border px-2 py-1 sticky left-0 bg-white z-10 whitespace-nowrap">
                                        {nurse.name}
                                        {team && (
                                          <span className={`ml-1 text-[10px] px-1 rounded ${TEAM_BG_COLORS[team] ?? ''}`}>
                                            {team}
                                          </span>
                                        )}
                                      </td>
                                      {shifts.map((s, dIdx) => {
                                        const isNightCell = ['夜', '管夜', '明', '管明'].includes(s);
                                        const teamCls = (isNightCell && team && TEAM_BG_COLORS[team]) || '';
                                        return (
                                          <td
                                            key={dIdx}
                                            className={`border px-1 py-0.5 text-center ${teamCls}`}
                                          >
                                            {s || ''}
                                          </td>
                                        );
                                      })}
                                    </tr>
                                  );
                                })}
                              </tbody>
                              {renderScheduleTfoot(draft.schedule_data, activeNurses, daysInMonth, targetYear, targetMonth, generateConfig)}
                            </table>
                          </div>
                          </>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          {view === 'generate' && <>

          {/* チーム未設定警告 */}
          {nursesWithoutTeam.length > 0 && (
            <div className="bg-yellow-50 border-l-4 border-yellow-400 rounded-xl p-3 mb-4">
              <div className="flex justify-between items-center gap-2">
                <span className="text-yellow-800 text-sm">
                  ⚠️ チーム未設定: <strong>{nursesWithoutTeam.length}</strong>名
                </span>
                <button
                  onClick={() => setShowUnassignedDetail(!showUnassignedDetail)}
                  className="text-xs px-2 py-1 bg-yellow-100 hover:bg-yellow-200 rounded text-yellow-900"
                >
                  {showUnassignedDetail ? '▲ 閉じる' : '▼ 詳細を表示'}
                </button>
              </div>
              {showUnassignedDetail && (
                <ul className="mt-2 text-sm text-yellow-900 space-y-0.5">
                  {nursesWithoutTeam.map(n => <li key={n.id}>• {n.name}</li>)}
                  <li className="mt-2 text-blue-700">
                    → ナース管理画面 (職員管理) でチームを設定できます
                  </li>
                </ul>
              )}
            </div>
          )}

          {/* 生成ボタン */}
          <div className="flex items-center gap-3 mb-4">
            <button
              type="button"
              onClick={generateTeamSchedule}
              disabled={loading}
              className="px-6 py-3 bg-gradient-to-r from-indigo-600 to-purple-600 text-white rounded-xl font-bold shadow-lg hover:shadow-xl transition-all disabled:opacity-50 flex items-center gap-2"
            >
              <RefreshCw size={18} className={loading ? 'animate-spin' : ''} />
              {loading ? (generatingPhase || '生成中...') : 'チーム編成で自動生成'}
            </button>
            {teamPatterns.length > 0 && (
              <span className="text-sm text-gray-500">
                {teamPatterns.length}パターン生成済
              </span>
            )}
          </div>

          {/* 生成中の長時間処理通知 */}
          {loading && (
            <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-4 shadow-sm">
              <div className="flex items-center gap-2 text-blue-900 font-bold">
                <RefreshCw size={18} className="animate-spin" />
                {generatingPhase || 'チーム編成で勤務表を生成中...'}
              </div>
              <p className="text-sm text-blue-700 mt-2">
                100%バランスを目指して最適化中。最大 <strong>2〜3分</strong> かかる場合があります。
                ブラウザを閉じずにお待ちください。
              </p>
              <progress className="w-full mt-2 h-2" />
            </div>
          )}

          {/* 改善提案 (3パターン共通の問題なので結果上部に1回表示) */}
          {teamPatterns.length > 0 && (() => {
            const tm0 = teamPatterns[0]?.metrics?.teamMetrics;
            return <ImprovementSuggestionsPanel teamMetrics={tm0} />;
          })()}

          {/* 結果表示 */}
          {teamPatterns.length > 0 && (
            <>
              <h3 className="font-bold text-lg mb-2 flex items-center gap-2">
                <Sparkles size={18} /> 生成結果
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                {teamPatterns.map((pat, idx) => {
                  const tm = pat.metrics?.teamMetrics ?? {};
                  const balanceRate = typeof tm.balanceRate === 'number' ? tm.balanceRate : 0;
                  const balancedDays = tm.balancedDays ?? 0;
                  const totalDays = tm.totalDays ?? 0;
                  const fallbackLv = tm.fallbackLevel ?? -1;
                  const perDay = tm.perDayTeamBalance ?? [];
                  const usedTeams = tm.usedTeams ?? [];
                  const hasError = !pat.data || Object.keys(pat.data).length === 0;
                  const ratePct = (balanceRate * 100).toFixed(0);
                  const m: any = pat.metrics ?? {};
                  // 緩和レベル/フォールバック状態のバッジ
                  let statusBadge: { text: string; cls: string } | null = null;
                  if (m.fallbackMode === 'error') {
                    statusBadge = { text: '❌ エラー', cls: 'bg-gray-200 text-gray-700' };
                  } else if (m.fallbackMode === 'greedy') {
                    statusBadge = { text: '⚠️ ベストエフォート', cls: 'bg-red-100 text-red-700' };
                  } else if (typeof m.relaxLevel === 'number') {
                    if (m.relaxLevel === 0) statusBadge = { text: '✅ 完全遵守', cls: 'bg-green-100 text-green-700' };
                    else if (m.relaxLevel === 1 || m.relaxLevel === 2) statusBadge = { text: '⚠️ 一部緩和', cls: 'bg-yellow-100 text-yellow-700' };
                    else if (m.relaxLevel === 3 || m.relaxLevel === 4) statusBadge = { text: '⚠️ 大幅緩和', cls: 'bg-orange-100 text-orange-700' };
                  }

                  return (
                    <div
                      key={idx}
                      className={`border-2 rounded-xl p-4 ${
                        hasError ? 'border-red-300 bg-red-50/40'
                        : balanceRate >= 0.8 ? 'border-green-400 bg-green-50/30'
                        : balanceRate >= 0.5 ? 'border-yellow-400 bg-yellow-50/30'
                        : 'border-orange-400 bg-orange-50/30'
                      }`}
                    >
                      {statusBadge && (
                        <div className="mb-2">
                          <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-bold ${statusBadge.cls}`}>{statusBadge.text}</span>
                        </div>
                      )}
                      <div className="flex justify-between items-center mb-2">
                        <h4 className="font-bold">{pat.label}</h4>
                        {hasError && <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded-full text-xs font-bold">解なし</span>}
                      </div>
                      {!hasError && m.warningMessage && (
                        <div className="mb-3 px-2 py-1.5 bg-amber-50 border border-amber-200 rounded text-xs text-amber-800">
                          {m.warningMessage}
                        </div>
                      )}

                      {hasError ? (
                        <div className="text-sm text-red-700 space-y-1">
                          <p>⚠️ このパターンは生成できませんでした</p>
                          {m.error && <p className="text-xs">{m.error}</p>}
                        </div>
                      ) : (
                        <>
                          <div className="space-y-1 text-sm mb-2">
                            <div className="flex justify-between">
                              <span className="text-gray-600">スコア</span>
                              <span className="font-bold">{pat.score?.toLocaleString?.() ?? pat.score}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">バランス達成率</span>
                              <span className="font-bold">{ratePct}% ({balancedDays}/{totalDays}日)</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">配分レベル</span>
                              <span className="text-xs">{fallbackLabel(fallbackLv)}</span>
                            </div>
                            {usedTeams.length > 0 && (
                              <div className="flex justify-between items-center">
                                <span className="text-gray-600">使用チーム</span>
                                <div className="flex gap-1">
                                  {usedTeams.map(t => (
                                    <span
                                      key={t}
                                      className={`text-[10px] px-1.5 py-0.5 rounded ${TEAM_BG_COLORS[t] ?? 'bg-gray-100'}`}
                                    >{t}</span>
                                  ))}
                                </div>
                              </div>
                            )}
                          </div>

                          <button
                            onClick={() => setShowTeamDetail(prev => prev.map((v, i) => i === idx ? !v : v))}
                            className="w-full text-xs px-2 py-1 bg-gray-100 hover:bg-gray-200 rounded text-gray-700 mb-2"
                          >
                            {showTeamDetail[idx] ? '▲ バランス詳細を閉じる' : '▼ バランス詳細を表示'}
                          </button>

                          {showTeamDetail[idx] && (
                            <div className="text-xs space-y-1 mb-3 max-h-48 overflow-y-auto bg-white/70 rounded p-2">
                              <p className="font-bold text-gray-700 mb-1">不均衡な日:</p>
                              {perDay.filter(d => !d.isBalanced).length === 0 ? (
                                <p className="text-green-700">✅ 全日均衡</p>
                              ) : (
                                <ul className="space-y-0.5">
                                  {perDay.filter(d => !d.isBalanced).map(d => (
                                    <li key={d.day} className="text-gray-700">
                                      <strong>{d.day}日:</strong>
                                      {d.missing.length > 0 && (
                                        <span className="text-red-700"> {d.missing.join(',')}が不在</span>
                                      )}
                                      {d.extra.length > 0 && (
                                        <span className="text-orange-700"> {d.extra.join(',')}が重複</span>
                                      )}
                                    </li>
                                  ))}
                                </ul>
                              )}
                            </div>
                          )}

                          <div className="flex gap-2">
                            <button
                              onClick={() => openSaveDialog(idx)}
                              className="flex-1 px-2 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-xs font-bold flex items-center justify-center gap-1"
                              title="下書きに保存 (既存勤務表は変更されません)"
                            >
                              <Save size={14} /> 下書き保存
                            </button>
                            <button
                              onClick={() => acceptPattern(idx)}
                              className="flex-1 px-2 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-xs font-bold"
                              title="既存勤務表に上書き保存"
                            >
                              ⭐ 正式採用
                            </button>
                          </div>
                        </>
                      )}
                    </div>
                  );
                })}
              </div>

              {/* チーム別カラープレビュー (採用前の確認用) */}
              <h3 className="font-bold text-lg mb-2">プレビュー (チーム色分け)</h3>
              <p className="text-xs text-gray-500 mb-2">
                夜勤セルをチーム別に色付け。チーム未設定は色なし。
              </p>
              {teamPatterns.map((pat, idx) => {
                if (!pat.data || Object.keys(pat.data).length === 0) return null;
                return (
                  <div key={idx} className="mb-4">
                    <h4 className="font-bold text-sm mb-1">{pat.label}</h4>
                    <div className="overflow-auto max-h-72 border rounded">
                      <table className="text-xs border-collapse">
                        <thead className="bg-gray-100 sticky top-0 z-20">
                          <tr>
                            <th className="border px-2 py-1 sticky left-0 bg-gray-100 z-30">氏名</th>
                            {Object.values(pat.data)[0]?.map((_, dIdx) => (
                              <th key={dIdx} className="border px-1 py-1 min-w-[28px]">{dIdx + 1}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {activeNurses.map(nurse => {
                            const shifts = pat.data[String(nurse.id)] || [];
                            const team = teamOfNurse[String(nurse.id)];
                            return (
                              <tr key={nurse.id}>
                                <td className="border px-2 py-1 sticky left-0 bg-white z-10 whitespace-nowrap">
                                  {nurse.name}
                                  {team && (
                                    <span className={`ml-1 text-[10px] px-1 rounded ${TEAM_BG_COLORS[team] ?? ''}`}>
                                      {team}
                                    </span>
                                  )}
                                </td>
                                {shifts.map((s, dIdx) => {
                                  const isNightCell = ['夜', '管夜', '明', '管明'].includes(s);
                                  const teamCls = (isNightCell && team && TEAM_BG_COLORS[team]) || '';
                                  return (
                                    <td
                                      key={dIdx}
                                      className={`border px-1 py-0.5 text-center relative ${teamCls}`}
                                    >
                                      {s || ''}
                                      {isNightCell && team && (
                                        <span className="absolute bottom-0 right-0 text-[8px] opacity-60 px-0.5">{team}</span>
                                      )}
                                    </td>
                                  );
                                })}
                              </tr>
                            );
                          })}
                        </tbody>
                        {renderScheduleTfoot(pat.data as Record<string, string[]>, activeNurses, daysInMonth, targetYear, targetMonth, generateConfig)}
                      </table>
                    </div>
                  </div>
                );
              })}
            </>
          )}

          </>}
          {/* === 下書き保存ダイアログ === */}
          {showSaveDialog && (
            <div className="fixed inset-0 bg-black/60 z-[60] flex items-center justify-center p-4">
              <div className="bg-white rounded-xl p-5 w-full max-w-md shadow-2xl">
                <h3 className="font-bold text-lg mb-3 flex items-center gap-2">
                  <Save size={18} /> 下書きを保存
                </h3>
                <p className="text-sm text-gray-600 mb-3">
                  この下書きに名前を付けて保存します。既存の勤務表は上書きされません。
                </p>
                <input
                  type="text"
                  value={draftName}
                  onChange={(e) => setDraftName(e.target.value)}
                  placeholder="例: ベテラン重視案"
                  maxLength={100}
                  autoFocus
                  className="w-full px-3 py-2 border-2 rounded-lg mb-4"
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') saveDraft();
                    if (e.key === 'Escape') setShowSaveDialog(false);
                  }}
                />
                <div className="flex justify-end gap-2">
                  <button
                    onClick={() => { setShowSaveDialog(false); setPendingPatternIndex(null); }}
                    className="px-4 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-lg text-sm"
                  >
                    キャンセル
                  </button>
                  <button
                    onClick={saveDraft}
                    className="px-4 py-2 bg-emerald-600 hover:bg-emerald-700 text-white rounded-lg text-sm font-bold"
                  >
                    保存
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
