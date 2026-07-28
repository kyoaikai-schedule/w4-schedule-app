import { createClient } from '@supabase/supabase-js';

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseAnonKey) {
  const missing = [
    !supabaseUrl && 'VITE_SUPABASE_URL',
    !supabaseAnonKey && 'VITE_SUPABASE_ANON_KEY',
  ].filter(Boolean).join(' / ');

  const message =
    `環境変数が設定されていません（${missing}）。管理者に連絡してください。`;

  // 白画面のまま原因不明にならないよう、画面にも表示してから throw する
  const root = document.getElementById('root');
  if (root) {
    root.innerHTML = `
      <div style="max-width:640px;margin:80px auto;padding:24px;border:2px solid #dc2626;border-radius:8px;font-family:sans-serif;color:#7f1d1d;background:#fef2f2;">
        <h1 style="font-size:20px;font-weight:bold;margin-bottom:12px;">設定エラー</h1>
        <p style="margin-bottom:8px;">${message}</p>
        <p style="font-size:13px;color:#991b1b;">
          Vercel の環境変数、またはローカルの <code>.env.local</code> に
          <code>VITE_SUPABASE_URL</code> と <code>VITE_SUPABASE_ANON_KEY</code> を設定してください。
        </p>
      </div>
    `;
  }

  throw new Error(message);
}

export const supabase = createClient(supabaseUrl, supabaseAnonKey);
