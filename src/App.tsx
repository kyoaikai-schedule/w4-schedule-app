import WardScheduleSystem from './WardScheduleSystem'

// 共愛会勤務表管理ポータル (admin-portal) URL
// VITE_PORTAL_URL で上書き可能。URL が変わったらここ 1 行を更新
const PORTAL_URL = import.meta.env.VITE_PORTAL_URL || 'https://admin-portal-five-psi.vercel.app'

function App() {
  const goToPortal = () => { window.location.href = PORTAL_URL }

  return (
    <>
      {/* 共通ヘッダーバー: 全画面 (システム選択・管理者ログイン・管理者画面・職員画面) で常に表示 */}
      <div className="bg-white border-b border-gray-200 px-4 py-2 sticky top-0 z-50 shadow-sm">
        <button
          onClick={goToPortal}
          className="text-sm text-blue-600 hover:text-blue-800 hover:underline transition-colors"
        >
          ← 共愛会勤務表管理ポータルに戻る
        </button>
      </div>

      <WardScheduleSystem />
    </>
  )
}

export default App
