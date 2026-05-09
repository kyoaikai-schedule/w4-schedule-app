/**
 * 夜勤チーム編成タブ (フェーズ3)
 *
 * フェーズ2 の /solve_team エンドポイントを呼び出し、teamMetrics を可視化する。
 * 既存の自動生成タブ・既存ロジックには一切干渉しない (本コンポーネント単体で完結)。
 *
 * - 親から渡される props で必要なデータと callback を受け取る
 * - /solve_team を呼び、teamPatterns に保持
 * - 採用時は親の onAcceptPattern callback を呼ぶ (= 既存の保存ロジック)
 */
import { useMemo, useState } from 'react';
import { Users, Sparkles, X, RefreshCw } from 'lucide-react';

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
}: Props) {
  const [teamPatterns, setTeamPatterns] = useState<TeamPattern[]>([]);
  const [showTeamDetail, setShowTeamDetail] = useState<boolean[]>([]);
  const [showUnassignedDetail, setShowUnassignedDetail] = useState(false);
  const [loading, setLoading] = useState(false);
  const [generatingPhase, setGeneratingPhase] = useState('');

  const nursesWithoutTeam = useMemo(
    () => activeNurses.filter(n => !n.team),
    [activeNurses]
  );

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
    if (!confirm(`${pattern.label} を採用して既存の勤務表に上書き保存しますか?`)) return;

    // data の key は string. 親側は Record<number, ...> を期待する箇所もあるが、
    // 既存実装では string でも number でも動作するので生のまま渡す。
    onAcceptPattern(pattern.data as Record<string, string[]>);
    alert('勤務表を保存しました。既存の自動生成タブで確認できます。');
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

                          <button
                            onClick={() => acceptPattern(idx)}
                            className="w-full px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-bold"
                          >
                            このパターンを採用
                          </button>
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
        </div>
      </div>
    </div>
  );
}
