/**
 * 夜勤チーム編成タブ (フェーズ3 + フェーズ4)
 *
 * フェーズ2 の /solve_team エンドポイントを呼び出し、teamMetrics を可視化する。
 * フェーズ4 で「下書き保存・管理」機能を追加 (schedule_drafts テーブル使用)。
 *
 * 既存の自動生成タブ・既存ロジックには一切干渉しない (本コンポーネント単体で完結)。
 */
import { useEffect, useMemo, useState } from 'react';
import { Users, Sparkles, X, RefreshCw, Save, FolderOpen, Trash2, Star, Eye } from 'lucide-react';
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
}

interface TeamPattern {
  label: string;
  data: Record<string, string[]>;
  score: number;
  metrics?: { teamMetrics?: TeamMetrics } & Record<string, unknown>;
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
}: Props) {
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
  const [previewDraftId, setPreviewDraftId] = useState<number | null>(null);

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
                    const isPreview = previewDraftId === draft.id;
                    return (
                      <div key={draft.id} className="border-2 rounded-xl p-3 bg-white">
                        <div className="flex justify-between items-start gap-3">
                          <div className="flex-1 min-w-0">
                            <h4 className="font-bold truncate">{draft.name}</h4>
                            <p className="text-xs text-gray-500">
                              作成: {formatDate(draft.created_at)}
                              {draft.source && ` (${draft.source})`}
                            </p>
                            <div className="mt-1 flex items-center gap-3 text-sm flex-wrap">
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
                          <div className="flex gap-1 shrink-0">
                            <button
                              onClick={() => setPreviewDraftId(isPreview ? null : draft.id)}
                              className="p-2 text-gray-600 hover:bg-gray-100 rounded"
                              title="プレビュー"
                            >
                              <Eye size={16} />
                            </button>
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
                        {isPreview && (
                          <div className="mt-3 overflow-auto max-h-72 border rounded">
                            <table className="text-xs border-collapse">
                              <thead className="bg-gray-100 sticky top-0">
                                <tr>
                                  <th className="border px-2 py-1 sticky left-0 bg-gray-100 z-10">氏名</th>
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
                            </table>
                          </div>
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
                      <div className="flex justify-between items-center mb-2">
                        <h4 className="font-bold">{pat.label}</h4>
                        {hasError && <span className="px-2 py-0.5 bg-red-100 text-red-700 rounded-full text-xs font-bold">解なし</span>}
                      </div>

                      {hasError ? (
                        <p className="text-sm text-red-700">⚠️ このパターンは生成できませんでした</p>
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
                        <thead className="bg-gray-100 sticky top-0">
                          <tr>
                            <th className="border px-2 py-1 sticky left-0 bg-gray-100 z-10">氏名</th>
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
