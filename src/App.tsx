import WardScheduleSystem from './WardScheduleSystem'

// 共愛会勤務表管理ポータル (admin-portal) URL
// VITE_PORTAL_URL で上書き可能。URL が変わったらここ 1 行を更新
const PORTAL_URL = import.meta.env.VITE_PORTAL_URL || 'https://admin-portal-five-psi.vercel.app'

function App() {
  return (
    <>
      <button
        onClick={() => { window.location.href = PORTAL_URL }}
        className="fixed top-2 right-2 z-50 px-3 py-1.5 bg-white/90 hover:bg-white text-xs text-gray-600 hover:text-gray-800 rounded-lg shadow-md border border-gray-200 transition-colors"
        title="共愛会勤務表管理ポータルに戻る"
      >
        ← ポータル
      </button>
      <WardScheduleSystem />
    </>
  )
}

export default App
