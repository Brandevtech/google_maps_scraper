#!/usr/bin/env python3
"""Interface Web Local para o Google Maps Scraper Kit.
Inclui:
- Autenticação com Google (Google Identity Services com modo Redirect nativo - zero erros de COOP/postMessage)
- Banco de dados SQLite persistente (users.db)
- Login automático enquanto a sessão for válida no navegador (cookie HttpOnly de 30 dias)
- Limite estrito de 3 usuários cadastrados no banco
- Isolamento total de resultados: cada usuário visualiza e baixa APENAS as suas próprias buscas!
"""

import csv
import datetime
import http.cookies
import http.server
import io
import json
import os
import secrets
import socketserver
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# ── Configurações e Variáveis de Ambiente ──────────────────────────────────────
def load_env_file():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("\"'")
                        if k not in os.environ:
                            os.environ[k] = v
        except Exception:
            pass

load_env_file()

BASE_API = os.environ.get("SCRAPER_BASE_URL") or f"http://localhost:{os.environ.get('SCRAPER_PORT') or '8080'}"
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
UA = "google-maps-scraper-kit-web/1.3"
PORT = int(os.environ.get("PORT") or 5000)
MAX_USERS = int(os.environ.get("MAX_USERS") or 3)
DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

# ── Banco de Dados SQLite ─────────────────────────────────────────────────────
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "andersonografia@gmail.com").strip().lower()

def is_admin(user):
    if not user or not user.get("email"):
        return False
    return user["email"].strip().lower() == ADMIN_EMAIL

def db_get_setting(key, default=None):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else default

def db_set_setting(key, value):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        conn.commit()

def get_max_users():
    val = db_get_setting("max_users")
    if val is not None:
        try:
            return int(val)
        except ValueError:
            pass
    return int(os.environ.get("MAX_USERS") or 3)

def db_delete_user(user_id):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM user_jobs WHERE user_id = ?", (user_id,))
        cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()

def db_get_all_users_admin():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT u.id, u.google_id, u.email, u.name, u.picture, u.created_at,
                   COUNT(j.id) as jobs_count
            FROM users u
            LEFT JOIN user_jobs j ON u.id = j.user_id
            GROUP BY u.id
            ORDER BY u.id ASC
        """)
        return [dict(r) for r in cur.fetchall()]

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                google_id TEXT UNIQUE NOT NULL,
                email TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                picture TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_id TEXT NOT NULL,
                job_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        """)
        conn.commit()

init_db()

def db_get_user_count():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM users")
        return cur.fetchone()[0]

def db_get_user_by_google_id(google_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE google_id = ?", (google_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def db_get_user_by_email(email):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE email = ?", (email,))
        row = cur.fetchone()
        return dict(row) if row else None

def db_get_all_users():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users ORDER BY id ASC")
        return [dict(r) for r in cur.fetchall()]

def db_get_user_by_id(user_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def db_create_user(google_id, email, name, picture):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (google_id, email, name, picture)
            VALUES (?, ?, ?, ?)
        """, (google_id, email, name, picture))
        user_id = cur.lastrowid
        conn.commit()
        return db_get_user_by_id(user_id)

def db_create_session(user_id, days=30):
    token = secrets.token_hex(32)
    expires_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sessions (token, user_id, expires_at)
            VALUES (?, ?, ?)
        """, (token, user_id, expires_at.isoformat()))
        conn.commit()
    return token

def db_get_user_by_session(token):
    if not token:
        return None
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT u.*, s.expires_at 
            FROM sessions s
            JOIN users u ON s.user_id = u.id
            WHERE s.token = ?
        """, (token,))
        row = cur.fetchone()
        if not row:
            return None
        expires_str = row["expires_at"]
        try:
            expires_at = datetime.datetime.fromisoformat(expires_str)
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=datetime.timezone.utc)
            if datetime.datetime.now(datetime.timezone.utc) > expires_at:
                cur.execute("DELETE FROM sessions WHERE token = ?", (token,))
                conn.commit()
                return None
        except Exception:
            return None
        return dict(row)

def db_delete_session(token):
    if not token:
        return
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM sessions WHERE token = ?", (token,))
        conn.commit()

# Funções de Isolamento de Buscas por Usuário
def db_add_user_job(user_id, job_id, job_name):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO user_jobs (user_id, job_id, job_name)
            VALUES (?, ?, ?)
        """, (user_id, job_id, job_name))
        conn.commit()

def db_get_user_jobs(user_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT job_id, job_name, created_at 
            FROM user_jobs 
            WHERE user_id = ? 
            ORDER BY id DESC
        """, (user_id,))
        return [dict(r) for r in cur.fetchall()]

def db_user_owns_job(user_id, job_id):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT 1 FROM user_jobs WHERE user_id = ? AND job_id = ?
        """, (user_id, job_id))
        return cur.fetchone() is not None

# ── Templates HTML ────────────────────────────────────────────────────────────

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="pt-BR" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Scraper de leads do Google Maps - Login</title>
  <link rel="icon" type="image/png" href="/assets/favicon.png">
  <script>
    (function() {
      const savedTheme = localStorage.getItem('brandev_theme') || 'dark';
      document.documentElement.setAttribute('data-theme', savedTheme);
    })();
  </script>
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <style>
    :root, [data-theme="dark"] {
      --primary: #0066ff;
      --primary-hover: #1d72fe;
      --navy: #f4f4f6;
      --bg: #121214;
      --bg-alt: #18181b;
      --surface: #202024;
      --surface-hover: #27272c;
      --border: #2e2e34;
      --border-focus: #0066ff;
      --text: #f4f4f6;
      --text-muted: #9ca3af;
      --card-shadow: 0 20px 35px -5px rgba(0, 0, 0, 0.6), 0 10px 15px -6px rgba(0, 0, 0, 0.5);
      --danger: #ef4444;
      --danger-bg: rgba(239, 68, 68, 0.15);
      --danger-text: #fca5a5;
      --success: #10b981;
      --btn-toggle-bg: #27272c;
      --btn-toggle-border: #383840;
    }

    [data-theme="light"] {
      --primary: #0066ff;
      --primary-hover: #0052cc;
      --navy: #0b192c;
      --bg: #ffffff;
      --bg-alt: #f8fafc;
      --surface: #ffffff;
      --surface-hover: #f1f5f9;
      --border: #e2e8f0;
      --border-focus: #0066ff;
      --text: #0f172a;
      --text-muted: #64748b;
      --card-shadow: 0 10px 25px -5px rgba(11, 25, 44, 0.08), 0 8px 10px -6px rgba(11, 25, 44, 0.04);
      --danger: #ef4444;
      --danger-bg: #fef2f2;
      --danger-text: #991b1b;
      --success: #10b981;
      --btn-toggle-bg: #ffffff;
      --btn-toggle-border: #e2e8f0;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body {
      background: var(--bg-alt);
      color: var(--text);
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      padding: 1.5rem;
      position: relative;
      transition: background 0.25s, color 0.25s;
    }
    .theme-topbar {
      position: absolute;
      top: 1.5rem;
      right: 1.5rem;
    }
    .btn-theme {
      background: var(--btn-toggle-bg);
      border: 1px solid var(--btn-toggle-border);
      color: var(--text);
      cursor: pointer;
      width: 40px;
      height: 40px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.15rem;
      transition: all 0.2s;
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
    }
    .btn-theme:hover {
      transform: scale(1.08);
      border-color: var(--border-focus);
    }
    [data-theme="dark"] .icon-sun { display: inline-block; }
    [data-theme="dark"] .icon-moon { display: none; }
    [data-theme="light"] .icon-sun { display: none; }
    [data-theme="light"] .icon-moon { display: inline-block; }

    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 1.25rem;
      width: 100%;
      max-width: 440px;
      padding: 2.6rem 2.2rem;
      box-shadow: var(--card-shadow);
      text-align: center;
      transition: background 0.25s, border-color 0.25s;
    }
    .logo-container {
      display: flex;
      justify-content: center;
      margin-bottom: 1.25rem;
    }
    .logo-img {
      width: 140px;
      height: 140px;
      object-fit: contain;
      filter: drop-shadow(0 8px 24px rgba(0, 102, 255, 0.25));
    }
    h1 {
      font-size: 1.35rem;
      font-weight: 700;
      color: var(--navy);
      letter-spacing: -0.02em;
      margin-bottom: 1.6rem;
      line-height: 1.3;
    }

    .btn-google-container {
      display: flex;
      justify-content: center;
      margin-bottom: 1rem;
      min-height: 44px;
    }

    .alert {
      padding: 0.75rem 1rem;
      border-radius: 0.5rem;
      font-size: 0.85rem;
      margin-top: 1rem;
      text-align: left;
      display: none;
    }
    .alert-danger {
      background: var(--danger-bg);
      border: 1px solid var(--danger);
      color: var(--danger-text);
      display: block;
    }
    .alert-success {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid var(--success);
      color: #86efac;
      display: block;
    }
  </style>
</head>
<body>
  <div class="theme-topbar">
    <button class="btn-theme" onclick="toggleTheme()" title="Alternar Tema Claro / Escuro">
      <span class="icon-sun">☀️</span>
      <span class="icon-moon">🌙</span>
    </button>
  </div>

  <div class="card">
    <div class="logo-container">
      <img src="/assets/logo.png" alt="Scraper de leads do Google Maps" class="logo-img">
    </div>
    <h1>Scraper de leads do Google Maps</h1>

    <!-- Botão Oficial do Google -->
    <div id="google-btn-section">
      <div id="g_id_onload"
           data-client_id="{{GOOGLE_CLIENT_ID}}"
           data-context="signin"
           data-callback="handleCredentialResponse"
           data-auto_prompt="false">
      </div>
      <div class="btn-google-container">
        <div class="g_id_signin"
             data-type="standard"
             data-shape="rectangular"
             data-theme="outline"
             data-text="signin_with"
             data-size="large"
             data-logo_alignment="left">
        </div>
      </div>
    </div>

    <!-- Mensagens de Alerta / Erro -->
    <div id="alert-box" class="alert">{{ERROR_MESSAGE}}</div>
  </div>

  <script>
    function toggleTheme() {
      const current = document.documentElement.getAttribute('data-theme') || 'dark';
      const next = current === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('brandev_theme', next);
    }

    async function handleCredentialResponse(response) {
      try {
        showAlert('Autenticando com o Google...', 'success');
        const res = await fetch('/api/auth/google', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ credential: response.credential })
        });
        const data = await res.json();
        if (res.ok) {
          showAlert('✅ Login efetuado com sucesso! Entrando...', 'success');
          window.location.replace('/');
          setTimeout(() => { window.location.href = '/'; }, 100);
          setTimeout(() => { window.location.reload(); }, 350);
        } else {
          showAlert(data.error || 'Erro ao realizar login.');
        }
      } catch (err) {
        showAlert('Erro de conexão ao autenticar: ' + err.message);
      }
    }

    function showAlert(msg, type = 'danger') {
      const box = document.getElementById('alert-box');
      box.textContent = msg;
      box.className = 'alert alert-' + type;
      box.style.display = 'block';
    }
  </script>
</body>
</html>"""

ADMIN_MODAL_HTML = """
  <div id="admin-modal" class="modal-overlay" onclick="if(event.target === this) closeAdminModal()">
    <div class="modal-card">
      <div class="modal-header">
        <div class="modal-title">
          <span>👑 Painel de Gerenciamento de Usuários</span>
        </div>
        <button class="btn-close-modal" onclick="closeAdminModal()">✕</button>
      </div>

      <!-- Configuração de Limite -->
      <div class="admin-box">
        <div class="admin-box-title">⚙️ Limite de Usuários Permitidos</div>
        <div style="font-size: 0.8rem; color: var(--text-muted); margin-bottom: 0.75rem;">
          Defina quantas contas Google podem ter acesso simultâneo ao sistema:
        </div>
        <div class="admin-limit-row">
          <input type="number" id="admin-limit-input" class="admin-limit-input" min="1" max="1000">
          <button class="btn-save-limit" onclick="saveMaxUsersLimit()">Salvar Novo Limite</button>
        </div>
      </div>

      <!-- Lista de Usuários -->
      <div class="admin-box" style="margin-bottom: 0;">
        <div class="admin-box-title">
          <span>👥 Usuários Autorizados (<span id="admin-user-count">0</span> de <span id="admin-max-count">0</span> vagas)</span>
        </div>
        <div id="admin-users-list" style="margin-top: 0.75rem;">
          <div style="text-align: center; color: var(--text-muted); padding: 1rem;">Carregando usuários...</div>
        </div>
      </div>
    </div>
  </div>
"""

APP_PAGE = """<!DOCTYPE html>
<html lang="pt-BR" data-theme="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Scraper de leads do Google Maps - Painel</title>
  <link rel="icon" type="image/png" href="/assets/favicon.png">
  <script>
    (function() {
      const savedTheme = localStorage.getItem('brandev_theme') || 'dark';
      document.documentElement.setAttribute('data-theme', savedTheme);
    })();
  </script>
  <style>
    :root, [data-theme="dark"] {
      --primary: #0066ff;
      --primary-hover: #1d72fe;
      --navy: #f4f4f6;
      --bg: #121214;
      --bg-alt: #18181b;
      --surface: #202024;
      --surface-hover: #27272c;
      --border: #2e2e34;
      --border-focus: #0066ff;
      --text: #f4f4f6;
      --text-muted: #9ca3af;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --input-bg: #161619;
      --input-border: #323238;
      --input-text: #f4f4f6;
      --table-header-bg: #18181b;
      --table-row-hover: rgba(255, 255, 255, 0.04);
      --badge-bg: #162620;
      --badge-text: #4ade80;
      --badge-border: #1d4a36;
      --user-profile-bg: #27272c;
      --btn-toggle-bg: #27272c;
      --btn-toggle-border: #383840;
      --card-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.5);
      --pill-ok-bg: #162620;
      --pill-ok-text: #4ade80;
      --pill-ok-border: #1d4a36;
      --pill-working-bg: #2b2214;
      --pill-working-text: #fbbf24;
      --pill-working-border: #4d3817;
      --pill-failed-bg: #2b1717;
      --pill-failed-text: #f87171;
      --pill-failed-border: #522020;
      --status-active-bg: #1c1f2e;
      --status-active-border: #2e3552;
      --status-active-text: #93c5fd;
      --status-done-bg: #162620;
      --status-done-border: #1d4a36;
      --status-done-text: #86efac;
      --status-error-bg: #2b1717;
      --status-error-border: #522020;
      --status-error-text: #fca5a5;
    }

    [data-theme="light"] {
      --primary: #0066ff;
      --primary-hover: #0052cc;
      --navy: #0b192c;
      --bg: #ffffff;
      --bg-alt: #f8fafc;
      --surface: #ffffff;
      --surface-hover: #f1f5f9;
      --border: #e2e8f0;
      --border-focus: #0066ff;
      --text: #0f172a;
      --text-muted: #64748b;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --input-bg: #ffffff;
      --input-border: #cbd5e1;
      --input-text: #0f172a;
      --table-header-bg: #f8fafc;
      --table-row-hover: #f8fafc;
      --badge-bg: #ecfdf5;
      --badge-text: #065f46;
      --badge-border: #a7f3d0;
      --user-profile-bg: #f8fafc;
      --btn-toggle-bg: #ffffff;
      --btn-toggle-border: #e2e8f0;
      --card-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
      --pill-ok-bg: #dcfce7;
      --pill-ok-text: #15803d;
      --pill-ok-border: #bbf7d0;
      --pill-working-bg: #fef3c7;
      --pill-working-text: #b45309;
      --pill-working-border: #fde68a;
      --pill-failed-bg: #fee2e2;
      --pill-failed-text: #b91c1c;
      --pill-failed-border: #fecaca;
      --status-active-bg: #eff6ff;
      --status-active-border: #bfdbfe;
      --status-active-text: #1e40af;
      --status-done-bg: #f0fdf4;
      --status-done-border: #bbf7d0;
      --status-done-text: #166534;
      --status-error-bg: #fef2f2;
      --status-error-border: #fecaca;
      --status-error-text: #991b1b;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body {
      background: var(--bg-alt);
      color: var(--text);
      padding: 1.75rem 2rem;
      min-height: 100vh;
      transition: background 0.25s, color 0.25s;
    }
    .container { max-width: 1140px; margin: 0 auto; }
    
    header {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 1rem;
      padding: 1rem 1.5rem;
      margin-bottom: 2rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 1rem;
      box-shadow: var(--card-shadow);
      transition: background 0.25s, border-color 0.25s;
    }
    .header-left { display: flex; align-items: center; gap: 1rem; }
    .brand-link { display: flex; align-items: center; gap: 0.85rem; text-decoration: none; color: inherit; }
    .brand-logo {
      width: 44px;
      height: 44px;
      object-fit: contain;
      filter: drop-shadow(0 2px 6px rgba(0, 102, 255, 0.35));
    }
    .brand-title {
      font-size: 1.25rem;
      font-weight: 800;
      color: var(--navy);
      letter-spacing: -0.02em;
      line-height: 1.1;
    }
    .brand-title span {
      color: var(--primary);
    }
    .brand-subtitle {
      font-size: 0.75rem;
      color: var(--text-muted);
      font-weight: 500;
      display: block;
    }
    .badge {
      font-size: 0.72rem;
      background: var(--badge-bg);
      color: var(--badge-text);
      border: 1px solid var(--badge-border);
      padding: 0.2rem 0.55rem;
      border-radius: 999px;
      font-weight: 600;
    }
    
    .header-right {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .btn-theme {
      background: var(--btn-toggle-bg);
      border: 1px solid var(--btn-toggle-border);
      color: var(--text);
      cursor: pointer;
      width: 38px;
      height: 38px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.1rem;
      transition: all 0.2s;
    }
    .btn-theme:hover {
      transform: scale(1.08);
      border-color: var(--border-focus);
    }
    [data-theme="dark"] .icon-sun { display: inline-block; }
    [data-theme="dark"] .icon-moon { display: none; }
    [data-theme="light"] .icon-sun { display: none; }
    [data-theme="light"] .icon-moon { display: inline-block; }

    .user-profile {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      background: var(--user-profile-bg);
      padding: 0.4rem 0.85rem;
      border-radius: 999px;
      border: 1px solid var(--border);
    }
    .user-avatar {
      width: 34px;
      height: 34px;
      border-radius: 50%;
      object-fit: cover;
      background: var(--primary);
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 700;
      font-size: 0.9rem;
      color: #fff;
    }
    .user-info { font-size: 0.82rem; line-height: 1.2; }
    .user-name { font-weight: 700; color: var(--navy); }
    .user-email { color: var(--text-muted); font-size: 0.74rem; }
    .btn-logout {
      background: none;
      border: none;
      color: var(--danger);
      cursor: pointer;
      font-size: 0.8rem;
      font-weight: 600;
      margin-left: 0.5rem;
      padding: 0.25rem 0.55rem;
      border-radius: 0.4rem;
      transition: 0.2s;
      display: flex;
      align-items: center;
      gap: 0.25rem;
    }
    .btn-logout:hover { background: rgba(239, 68, 68, 0.15); }

    .layout { display: grid; grid-template-columns: 380px 1fr; gap: 2rem; }
    @media (max-width: 890px) { .layout { grid-template-columns: 1fr; } }
    
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 1rem;
      padding: 1.6rem;
      box-shadow: var(--card-shadow);
      transition: background 0.25s, border-color 0.25s;
    }
    .card h2 {
      font-size: 1.18rem;
      font-weight: 700;
      margin-bottom: 1.25rem;
      color: var(--navy);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .form-group { margin-bottom: 1.25rem; }
    label {
      display: block;
      font-size: 0.85rem;
      font-weight: 600;
      margin-bottom: 0.45rem;
      color: var(--text-muted);
    }
    input[type="text"], input[type="number"] {
      width: 100%;
      padding: 0.7rem 0.9rem;
      background: var(--input-bg);
      border: 1px solid var(--input-border);
      border-radius: 0.55rem;
      color: var(--input-text);
      font-size: 0.95rem;
      transition: 0.15s;
    }
    input:focus {
      outline: none;
      border-color: var(--border-focus);
      box-shadow: 0 0 0 3px rgba(0, 102, 255, 0.25);
    }
    .help-text { font-size: 0.76rem; color: var(--text-muted); margin-top: 0.35rem; }
    
    button.btn {
      width: 100%;
      padding: 0.8rem;
      background: linear-gradient(135deg, #0066ff 0%, #0052cc 100%);
      color: white;
      border: none;
      border-radius: 0.55rem;
      font-weight: 600;
      font-size: 0.95rem;
      cursor: pointer;
      transition: 0.2s;
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 0.5rem;
      box-shadow: 0 4px 12px rgba(0, 102, 255, 0.3);
    }
    button.btn:hover {
      box-shadow: 0 6px 16px rgba(0, 102, 255, 0.45);
      filter: brightness(1.08);
    }
    button.btn:disabled { opacity: 0.55; cursor: not-allowed; box-shadow: none; filter: none; }
    
    .status-box { margin-top: 1.25rem; padding: 0.85rem; border-radius: 0.55rem; font-size: 0.88rem; display: none; line-height: 1.4; }
    .status-box.active { display: block; background: var(--status-active-bg); border: 1px solid var(--status-active-border); color: var(--status-active-text); }
    .status-box.done { display: block; background: var(--status-done-bg); border: 1px solid var(--status-done-border); color: var(--status-done-text); }
    .status-box.error { display: block; background: var(--status-error-bg); border: 1px solid var(--status-error-border); color: var(--status-error-text); }
    
    table { width: 100%; border-collapse: collapse; margin-top: 0.75rem; font-size: 0.875rem; }
    th, td { text-align: left; padding: 0.85rem 0.75rem; border-bottom: 1px solid var(--border); }
    th { color: var(--text-muted); font-weight: 600; background: var(--table-header-bg); }
    tr:hover { background: var(--table-row-hover); }
    
    .status-pill { padding: 0.22rem 0.65rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; display: inline-block; }
    .pill-ok { background: var(--pill-ok-bg); color: var(--pill-ok-text); border: 1px solid var(--pill-ok-border); }
    .pill-working { background: var(--pill-working-bg); color: var(--pill-working-text); border: 1px solid var(--pill-working-border); }
    .pill-failed { background: var(--pill-failed-bg); color: var(--pill-failed-text); border: 1px solid var(--pill-failed-border); }
    
    .btn-download {
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.4rem 0.85rem;
      background: var(--primary);
      color: white;
      border-radius: 0.45rem;
      text-decoration: none;
      font-size: 0.78rem;
      font-weight: 600;
      transition: 0.15s;
    }
    .btn-download:hover { background: var(--primary-hover); }
    
    .spinner {
      border: 2px solid rgba(255,255,255,0.3);
      border-left-color: #fff;
      border-radius: 50%;
      width: 16px;
      height: 16px;
      animation: spin 0.8s linear infinite;
      display: inline-block;
    }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
    .btn-admin-nav {
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
      padding: 0.45rem 0.95rem;
      background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
      color: #ffffff;
      border: none;
      border-radius: 999px;
      font-size: 0.8rem;
      font-weight: 700;
      cursor: pointer;
      transition: all 0.2s;
      box-shadow: 0 2px 8px rgba(37, 99, 235, 0.35);
    }
    .btn-admin-nav:hover {
      transform: translateY(-1px);
      box-shadow: 0 4px 14px rgba(37, 99, 235, 0.5);
      filter: brightness(1.08);
    }
    .badge-admin {
      display: inline-block;
      padding: 0.15rem 0.5rem;
      background: #3b82f6;
      color: #ffffff;
      border-radius: 999px;
      font-size: 0.68rem;
      font-weight: 700;
      margin-left: 0.35rem;
      vertical-align: middle;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    /* Modal de Administrador */
    .modal-overlay {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.75);
      backdrop-filter: blur(5px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
      padding: 1.5rem;
      animation: fadeIn 0.15s ease-out;
    }
    .modal-card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 1.25rem;
      width: 100%;
      max-width: 680px;
      max-height: 90vh;
      overflow-y: auto;
      padding: 2rem;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.7);
    }
    .modal-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid var(--border);
      padding-bottom: 1.25rem;
      margin-bottom: 1.5rem;
    }
    .modal-title {
      font-size: 1.25rem;
      font-weight: 800;
      color: var(--navy);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .btn-close-modal {
      background: none;
      border: none;
      color: var(--text-muted);
      font-size: 1.5rem;
      cursor: pointer;
      line-height: 1;
      padding: 0.2rem 0.5rem;
      border-radius: 0.4rem;
      transition: 0.2s;
    }
    .btn-close-modal:hover {
      color: var(--text);
      background: var(--surface-hover);
    }
    .admin-box {
      background: var(--bg-alt);
      border: 1px solid var(--border);
      border-radius: 0.85rem;
      padding: 1.25rem;
      margin-bottom: 1.5rem;
    }
    .admin-box-title {
      font-size: 0.92rem;
      font-weight: 700;
      color: var(--navy);
      margin-bottom: 0.75rem;
      display: flex;
      align-items: center;
      gap: 0.4rem;
    }
    .admin-limit-row {
      display: flex;
      align-items: center;
      gap: 0.75rem;
      flex-wrap: wrap;
    }
    .admin-limit-input {
      width: 100px !important;
      text-align: center;
      font-weight: 700;
      font-size: 1rem;
    }
    .btn-save-limit {
      padding: 0.65rem 1.25rem;
      background: var(--primary);
      color: white;
      border: none;
      border-radius: 0.5rem;
      font-weight: 700;
      font-size: 0.85rem;
      cursor: pointer;
      transition: 0.2s;
    }
    .btn-save-limit:hover {
      background: var(--primary-hover);
    }
    .user-item-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0.85rem;
      border-bottom: 1px solid var(--border);
      gap: 0.75rem;
      transition: background 0.15s;
    }
    .user-item-row:last-child {
      border-bottom: none;
    }
    .user-item-row:hover {
      background: var(--surface-hover);
      border-radius: 0.5rem;
    }
    .user-item-left {
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }
    .btn-remove-user {
      padding: 0.4rem 0.75rem;
      background: rgba(239, 68, 68, 0.15);
      border: 1px solid #ef4444;
      color: #ef4444;
      border-radius: 0.45rem;
      font-size: 0.75rem;
      font-weight: 700;
      cursor: pointer;
      transition: 0.2s;
    }
    .btn-remove-user:hover {
      background: #ef4444;
      color: white;
    }
    .badge-owner {
      font-size: 0.72rem;
      font-weight: 700;
      background: rgba(37, 99, 235, 0.2);
      color: #3b82f6;
      border: 1px solid #2563eb;
      padding: 0.25rem 0.6rem;
      border-radius: 999px;
    }
    @keyframes fadeIn {
      from { opacity: 0; transform: scale(0.98); }
      to { opacity: 1; transform: scale(1); }
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="header-left">
        <a href="/" class="brand-link">
          <img src="/assets/logo.png" alt="Scraper de leads do Google Maps" class="brand-logo">
          <div class="brand-titles">
            <div class="brand-title">Scraper <span>Brandev</span></div>
            <span class="brand-subtitle">Google Maps Lead Extractor</span>
          </div>
        </a>
        <span class="badge">Área Privada</span>
      </div>

      <div class="header-right">
        {{ADMIN_BUTTON}}
        <button class="btn-theme" onclick="toggleTheme()" title="Alternar Tema Claro / Escuro">
          <span class="icon-sun">☀️</span>
          <span class="icon-moon">🌙</span>
        </button>

        <div class="user-profile">
          {{USER_AVATAR}}
          <div class="user-info">
            <div class="user-name">{{USER_NAME}}{{ADMIN_BADGE}}</div>
            <div class="user-email">{{USER_EMAIL}}</div>
          </div>
          <button onclick="logout()" class="btn-logout" title="Sair do painel">Sair 🚪</button>
        </div>
      </div>
    </header>

    <div class="layout">
      <!-- FORMULÁRIO -->
      <div class="card">
        <h2>🚀 Nova Busca de Leads</h2>
        <form id="scrape-form" onsubmit="startScrape(event)">
          <div class="form-group">
            <label for="keyword">O que buscar? (Nicho / Palavra-chave)</label>
            <input type="text" id="keyword" required placeholder="Ex: padarias, dentistas, academias">
          </div>

          <div class="form-group">
            <label for="city">Cidade e Estado</label>
            <input type="text" id="city" required placeholder="Ex: Curitiba, PR ou São Paulo, SP">
            <div class="help-text">✨ Geocodificação automática por nome (sem latitude manual).</div>
          </div>

          <div class="form-group">
            <label for="depth">Profundidade (Qtd. de páginas / rolagens)</label>
            <input type="number" id="depth" value="2" min="1" max="20">
            <div class="help-text">1 a 3 para testes rápidos; 5 a 10 para listas completas.</div>
          </div>

          <button type="submit" id="submit-btn" class="btn">
            <span>Iniciar Busca</span>
          </button>
        </form>

        <div id="status-box" class="status-box">
          <div id="status-text"></div>
        </div>
      </div>

      <!-- TABELA DE RESULTADOS -->
      <div class="card">
        <h2>📊 Suas Extrações Recentes</h2>
        <table id="jobs-table">
          <thead>
            <tr>
              <th>Busca</th>
              <th>Horário</th>
              <th>Status</th>
              <th>Ação</th>
            </tr>
          </thead>
          <tbody id="jobs-tbody">
            <tr>
              <td colspan="4" style="text-align: center; color: var(--text-muted); padding: 2rem;">
                Carregando suas buscas...
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  {{ADMIN_MODAL}}

  <script>
    let activePollInterval = null;

    function toggleTheme() {
      const current = document.documentElement.getAttribute('data-theme') || 'dark';
      const next = current === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', next);
      localStorage.setItem('brandev_theme', next);
    }

    async function logout() {
      try {
        await fetch('/api/auth/logout', { method: 'POST' });
      } catch (err) {
        console.warn("Servidor offline ou erro no logout:", err);
      }
      document.cookie = "session_id=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT;";
      window.location.href = '/login';
    }

    async function loadJobs() {
      try {
        const res = await fetch('/api/jobs');
        if (res.status === 401) {
          window.location.href = '/login';
          return;
        }
        const jobs = await res.json();
        const tbody = document.getElementById('jobs-tbody');
        if (!jobs || jobs.length === 0) {
          tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: var(--text-muted); padding: 2rem;">Você ainda não fez nenhuma busca. Inicie uma nova busca ao lado!</td></tr>';
          return;
        }
        tbody.innerHTML = jobs.map(j => {
          let pill = '<span class="status-pill pill-working">em andamento</span>';
          let actions = '<span style="color: var(--text-muted); font-size: 0.8rem;">Aguardando...</span>';
          if (j.Status === 'ok') {
            pill = '<span class="status-pill pill-ok">concluído</span>';
            actions = `<a href="/api/download?id=${j.ID}" class="btn-download" target="_blank">⬇ Baixar CSV</a>`;
          } else if (j.Status === 'failed') {
            pill = '<span class="status-pill pill-failed">falhou</span>';
            actions = `<span style="color: #ef4444; font-size: 0.8rem;">Erro</span>`;
          }
          const dateStr = j.Date ? new Date(j.Date).toLocaleTimeString() : '-';
          return `
            <tr>
              <td><strong>${j.Name || 'Sem nome'}</strong></td>
              <td>${dateStr}</td>
              <td>${pill}</td>
              <td>${actions}</td>
            </tr>
          `;
        }).join('');
      } catch (err) {
        console.warn('Servidor offline ao carregar jobs:', err);
      }
    }

    async function startScrape(e) {
      e.preventDefault();
      const btn = document.getElementById('submit-btn');
      const statusBox = document.getElementById('status-box');
      const statusText = document.getElementById('status-text');

      const keyword = document.getElementById('keyword').value.trim();
      const city = document.getElementById('city').value.trim();
      const depth = parseInt(document.getElementById('depth').value) || 2;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span> <span>Geocodificando e iniciando...</span>';
      statusBox.className = 'status-box active';
      statusText.innerHTML = `Localizando <strong>${city}</strong> e criando tarefa no scraper...`;

      try {
        const res = await fetch('/api/start', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ keyword, city, depth })
        });
        if (res.status === 401) {
          window.location.href = '/login';
          return;
        }
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao iniciar busca');

        statusText.innerHTML = `Busca <strong>${data.name}</strong> em andamento! Acompanhe na tabela ao lado.`;
        loadJobs();

        if (activePollInterval) clearInterval(activePollInterval);
        activePollInterval = setInterval(async () => {
          try {
            const pollRes = await fetch(`/api/job?id=${data.id}`);
            const job = await pollRes.json();
            loadJobs();
            if (job.Status === 'ok') {
              clearInterval(activePollInterval);
              statusBox.className = 'status-box done';
              statusText.innerHTML = `✅ Busca concluída com sucesso! Clique em <strong>Baixar CSV</strong> na tabela.`;
              btn.disabled = false;
              btn.innerHTML = '<span>Iniciar Nova Busca</span>';
            } else if (job.Status === 'failed') {
              clearInterval(activePollInterval);
              statusBox.className = 'status-box error';
              statusText.innerHTML = `❌ A busca falhou. Tente novamente em alguns instantes.`;
              btn.disabled = false;
              btn.innerHTML = '<span>Tentar Novamente</span>';
            }
          } catch (e) {
            console.warn("Polling de status:", e);
          }
        }, 5000);

      } catch (err) {
        statusBox.className = 'status-box error';
        statusText.innerHTML = `Erro: ${err.message}`;
        btn.disabled = false;
        btn.innerHTML = '<span>Iniciar Busca</span>';
      }
    }

    loadJobs();
    setInterval(loadJobs, 10000);

    // Funções de Gestão de Usuários (Exclusivo Administrador)
    function openAdminModal() {
      const modal = document.getElementById('admin-modal');
      if (modal) {
        modal.style.display = 'flex';
        loadAdminData();
      }
    }

    function closeAdminModal() {
      const modal = document.getElementById('admin-modal');
      if (modal) modal.style.display = 'none';
    }

    async function loadAdminData() {
      try {
        const res = await fetch('/api/admin/users');
        if (!res.ok) return;
        const data = await res.json();
        
        document.getElementById('admin-limit-input').value = data.max_users;
        document.getElementById('admin-user-count').textContent = data.users.length;
        document.getElementById('admin-max-count').textContent = data.max_users;

        const listContainer = document.getElementById('admin-users-list');
        listContainer.innerHTML = data.users.map(u => {
          const isMaster = u.email.toLowerCase() === data.admin_email.toLowerCase();
          const avatar = u.picture ? `<img src="${u.picture}" style="width: 32px; height: 32px; border-radius: 50%; object-fit: cover;">` : `<div style="width: 32px; height: 32px; border-radius: 50%; background: #0066ff; color: white; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 0.8rem;">${(u.name || u.email)[0].toUpperCase()}</div>`;
          const actionBtn = isMaster 
            ? `<span class="badge-owner">🛡️ Admin Principal</span>`
            : `<button class="btn-remove-user" onclick="deleteUserAccount(${u.id}, '${u.name || u.email}')">Remover Acesso 🗑️</button>`;

          return `
            <div class="user-item-row">
              <div class="user-item-left">
                ${avatar}
                <div>
                  <div style="font-weight: 700; font-size: 0.88rem; color: var(--navy);">${u.name}</div>
                  <div style="font-size: 0.74rem; color: var(--text-muted);">${u.email} • ${u.jobs_count} busca(s) realizada(s)</div>
                </div>
              </div>
              <div>${actionBtn}</div>
            </div>
          `;
        }).join('');
      } catch (err) {
        console.error("Erro ao carregar dados do admin:", err);
      }
    }

    async function saveMaxUsersLimit() {
      const input = document.getElementById('admin-limit-input');
      const val = parseInt(input.value);
      if (!val || val < 1) {
        alert("O limite deve ser de pelo menos 1 usuário.");
        return;
      }
      try {
        const res = await fetch('/api/admin/settings/limit', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ max_users: val })
        });
        const data = await res.json();
        if (res.ok) {
          alert(`✅ Limite atualizado para ${data.max_users} usuários com sucesso!`);
          loadAdminData();
        } else {
          alert("Erro: " + (data.error || "Falha ao salvar limite."));
        }
      } catch (err) {
        alert("Erro de conexão ao salvar limite.");
      }
    }

    async function deleteUserAccount(userId, userName) {
      if (!confirm(`Tem certeza que deseja remover o acesso de "${userName}"? As buscas e sessões deste usuário serão apagadas.`)) {
        return;
      }
      try {
        const res = await fetch('/api/admin/users/delete', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ user_id: userId })
        });
        const data = await res.json();
        if (res.ok) {
          alert("✅ Usuário removido com sucesso!");
          loadAdminData();
        } else {
          alert("Erro ao remover: " + (data.error || "Falha"));
        }
      } catch (err) {
        alert("Erro de rede ao remover usuário.");
      }
    }

  </script>
</body>
</html>"""

# ── Funções Auxiliares ────────────────────────────────────────────────────────
def geocode_city(place):
    q = urllib.parse.urlencode({"format": "json", "limit": 1, "q": place})
    url = f"https://nominatim.openstreetmap.org/search?{q}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=15) as resp:
        hits = json.loads(resp.read().decode())
        if hits:
            return str(hits[0]["lat"]), str(hits[0]["lon"])
    return None, None

def scraper_req(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", "User-Agent": UA}
    req = urllib.request.Request(BASE_API + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()

def verify_google_token(id_token):
    """Valida o token JWT diretamente com a API oficial do Google"""
    url = f"https://oauth2.googleapis.com/tokeninfo?id_token={urllib.parse.quote(id_token)}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "sub" in data and "email" in data:
                return data
    except Exception as e:
        print(f"Erro ao validar token Google: {e}", file=sys.stderr)
    return None

def process_google_credential(credential):
    """Processa o credential do Google, valida limite de 3 usuários e retorna (user, session_token, error_msg)"""
    token_data = verify_google_token(credential)
    if not token_data:
        return None, None, "Token do Google inválido ou expirado."

    google_id = token_data["sub"]
    email = token_data["email"].lower()
    name = token_data.get("name") or email.split("@")[0]
    picture = token_data.get("picture", "")

    user = db_get_user_by_google_id(google_id) or db_get_user_by_email(email)
    if not user:
        if db_get_user_count() >= get_max_users():
            return None, None, "Limite máximo de 3 usuários atingido no sistema. Novos cadastros estão bloqueados."
        user = db_create_user(google_id, email, name, picture)

    session_token = db_create_session(user["id"], days=30)
    return user, session_token, None

# ── Servidor HTTP ─────────────────────────────────────────────────────────────
def get_cookie_value(cookie_header, name):
    if not cookie_header:
        return None
    for part in cookie_header.split(";"):
        part = part.strip()
        if part.startswith(name + "="):
            val = part[len(name) + 1:].strip()
            if val.startswith('"') and val.endswith('"') and len(val) >= 2:
                val = val[1:-1]
            return val
    return None

class AuthHandler(http.server.BaseHTTPRequestHandler):
    def get_session_user(self):
        cookie_header = self.headers.get("Cookie", "")
        token = get_cookie_value(cookie_header, "session_id")
        if not token:
            return None
        return db_get_user_by_session(token)

    def send_session_cookie(self, token, max_age=2592000): # 30 dias
        cookie = f"session_id={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}"
        self.send_header("Set-Cookie", cookie)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        # Servir assets estáticos (logo, favicon) publicamente
        if parsed.path == "/favicon.ico":
            fav_path = os.path.join(os.path.dirname(__file__), "assets", "favicon.png")
            if os.path.exists(fav_path):
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                with open(fav_path, "rb") as f:
                    self.wfile.write(f.read())
                return

        if parsed.path.startswith("/assets/"):
            asset_name = os.path.basename(parsed.path)
            asset_path = os.path.join(os.path.dirname(__file__), "assets", asset_name)
            if os.path.exists(asset_path) and os.path.isfile(asset_path):
                ext = os.path.splitext(asset_name)[1].lower()
                mime = "image/png"
                if ext in [".jpg", ".jpeg"]:
                    mime = "image/jpeg"
                elif ext == ".svg":
                    mime = "image/svg+xml"
                elif ext == ".ico":
                    mime = "image/x-icon"
                self.send_response(200)
                self.send_header("Content-Type", mime)
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                with open(asset_path, "rb") as f:
                    self.wfile.write(f.read())
                return

        user = self.get_session_user()

        # Rota da tela de login
        if parsed.path == "/login":
            if user:
                self.send_response(302)
                self.send_header("Location", "/")
                self.end_headers()
                return

            qs = urllib.parse.parse_qs(parsed.query)
            err = qs.get("error", [""])[0]
            err_html = f'<div class="alert alert-danger" style="display:block;">{err}</div>' if err else ''

            html = LOGIN_PAGE.replace("{{GOOGLE_CLIENT_ID}}", GOOGLE_CLIENT_ID)
            html = html.replace("{{ERROR_MESSAGE}}", err_html)



            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Referrer-Policy", "no-referrer-when-downgrade")
            self.send_header("Cross-Origin-Opener-Policy", "same-origin-allow-popups")
            self.send_header("Cross-Origin-Embedder-Policy", "unsafe-none")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        # Rota de logout direto via GET
        elif parsed.path == "/logout":
            cookie_header = self.headers.get("Cookie", "")
            token = get_cookie_value(cookie_header, "session_id")
            if token:
                db_delete_session(token)
            self.send_response(302)
            self.send_header("Set-Cookie", "session_id=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT; HttpOnly; SameSite=Lax")
            self.send_header("Location", "/login")
            self.end_headers()
            return

        # Status da cota de usuários
        elif parsed.path == "/api/auth/status":
            count = db_get_user_count()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "user_count": count,
                "max_users": get_max_users(),
                "is_full": count >= get_max_users(),
                "google_configured": bool(GOOGLE_CLIENT_ID)
            }).encode())
            return

        # Todas as outras rotas exigem sessão válida!
        if not user:
            if parsed.path.startswith("/api/"):
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Não autorizado. Faça login primeiro."}).encode())
            else:
                self.send_response(302)
                self.send_header("Location", "/login")
                self.end_headers()
            return

        # Painel Principal (/)
        if parsed.path == "/" or parsed.path == "/index.html":
            avatar_html = f'<img src="{user["picture"]}" class="user-avatar" alt="Avatar">' if user.get("picture") else f'<div class="user-avatar">{user["name"][:1].upper()}</div>'
            admin_btn = '<button class="btn-admin-nav" onclick="openAdminModal()">⚙️ Gerenciar Usuários</button>' if is_admin(user) else ''
            admin_badge = '<span class="badge-admin">Admin</span>' if is_admin(user) else ''
            admin_modal = ADMIN_MODAL_HTML if is_admin(user) else ''
            
            html = APP_PAGE.replace("{{USER_NAME}}", user["name"]).replace("{{USER_EMAIL}}", user["email"]).replace("{{USER_AVATAR}}", avatar_html)
            html = html.replace("{{ADMIN_BUTTON}}", admin_btn).replace("{{ADMIN_BADGE}}", admin_badge).replace("{{ADMIN_MODAL}}", admin_modal)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        elif parsed.path == "/api/admin/users":
            if not is_admin(user):
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Acesso negado: exclusivo para administrador."}).encode())
                return
            users_data = db_get_all_users_admin()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "users": users_data,
                "max_users": get_max_users(),
                "admin_email": ADMIN_EMAIL
            }).encode())
            return

        # Listagem de jobs ISOLADA por usuário
        elif parsed.path == "/api/jobs":
            try:
                # 1. Recupera do SQLite apenas as buscas criadas por este usuário
                my_db_jobs = db_get_user_jobs(user["id"])
                if not my_db_jobs:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(b"[]")
                    return

                # 2. Busca lista do motor gosom
                raw = scraper_req("GET", "/api/v1/jobs")
                all_engine_jobs = json.loads(raw) or []
                engine_jobs_map = {j["ID"]: j for j in all_engine_jobs if "ID" in j}

                # 3. Filtra apenas os jobs do usuário
                user_job_list = []
                for db_j in my_db_jobs:
                    jid = db_j["job_id"]
                    if jid in engine_jobs_map:
                        user_job_list.append(engine_jobs_map[jid])
                    else:
                        # Consulta o status direto caso ainda não esteja na lista principal
                        try:
                            item_raw = scraper_req("GET", f"/api/v1/jobs/{jid}")
                            item_data = json.loads(item_raw)
                            if item_data:
                                user_job_list.append(item_data)
                        except Exception:
                            user_job_list.append({
                                "ID": jid,
                                "Name": db_j["job_name"],
                                "Date": db_j["created_at"],
                                "Status": "working"
                            })

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(user_job_list).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        elif parsed.path == "/api/job":
            qs = urllib.parse.parse_qs(parsed.query)
            job_id = qs.get("id", [""])[0]
            # Validação de isolamento: usuário só acessa job próprio!
            if not db_user_owns_job(user["id"], job_id):
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Acesso negado: este resultado pertence a outro usuário."}).encode())
                return
            try:
                raw = scraper_req("GET", f"/api/v1/jobs/{job_id}")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(raw)
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        elif parsed.path == "/api/download":
            qs = urllib.parse.parse_qs(parsed.query)
            job_id = qs.get("id", [""])[0]
            # Validação de isolamento: usuário só baixa lead de busca própria!
            if not db_user_owns_job(user["id"], job_id):
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Acesso negado: este arquivo pertence a outro usuário."}).encode())
                return
            try:
                req = urllib.request.Request(f"{BASE_API}/api/v1/jobs/{job_id}/download")
                with urllib.request.urlopen(req, timeout=30) as resp:
                    data = resp.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/csv; charset=utf-8")
                self.send_header("Content-Disposition", f'attachment; filename="leads-{job_id[:8]}.csv"')
                self.end_headers()
                self.wfile.write(data)
            except Exception as e:
                self.send_response(500)
                self.wfile.write(str(e).encode())
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        content_len = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_len) if content_len > 0 else b""

        # 1. Callback de Login do Google via Redirect (Nativo do Google GIS)
        if parsed.path == "/login":
            form_data = urllib.parse.parse_qs(body_bytes.decode("utf-8", "replace"))
            credential = form_data.get("credential", [""])[0]
            if not credential:
                self.send_response(302)
                self.send_header("Location", "/login?error=" + urllib.parse.quote("Credencial do Google não recebida."))
                self.end_headers()
                return

            user, session_token, error_msg = process_google_credential(credential)
            if error_msg:
                self.send_response(302)
                self.send_header("Location", "/login?error=" + urllib.parse.quote(error_msg))
                self.end_headers()
                return

            # Login efetuado com sucesso -> Redireciona para o painel com cookie de 30 dias!
            self.send_response(302)
            self.send_header("Location", "/")
            self.send_session_cookie(session_token)
            self.end_headers()
            return

        # 2. Login Google via API JSON (Popup fallback)
        elif parsed.path == "/api/auth/google":
            try:
                data = json.loads(body_bytes.decode())
                credential = data.get("credential")
                user, session_token, error_msg = process_google_credential(credential)
                if error_msg:
                    status_code = 403 if "Limite" in error_msg else 401
                    self.send_response(status_code)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": error_msg}).encode())
                    return

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_session_cookie(session_token)
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "user": {"name": user["name"], "email": user["email"]}}).encode())
                return
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

        
        # 4. Logout
        elif parsed.path == "/api/auth/logout":
            cookie_header = self.headers.get("Cookie", "")
            token = get_cookie_value(cookie_header, "session_id")
            if token:
                db_delete_session(token)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Set-Cookie", "session_id=; Path=/; Expires=Thu, 01 Jan 1970 00:00:00 GMT")
            self.end_headers()
            self.wfile.write(json.dumps({"ok": True}).encode())
            return

        # Rotas autenticadas
        user = self.get_session_user()
        if not user:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Não autorizado."}).encode())
            return

        elif parsed.path == "/api/admin/users/delete":
            if not is_admin(user):
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Acesso negado: exclusivo para administrador."}).encode())
                return
            try:
                data = json.loads(body_bytes.decode())
                target_id = int(data.get("user_id"))
                target_user = db_get_user_by_id(target_id)
                if not target_user:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Usuário não encontrado."}).encode())
                    return
                if target_user["email"].lower() == ADMIN_EMAIL:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Você não pode excluir o perfil de administrador principal."}).encode())
                    return
                db_delete_user(target_id)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True}).encode())
                return
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

        elif parsed.path == "/api/admin/settings/limit":
            if not is_admin(user):
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Acesso negado: exclusivo para administrador."}).encode())
                return
            try:
                data = json.loads(body_bytes.decode())
                limit = int(data.get("max_users", 3))
                if limit < 1 or limit > 1000:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "O limite deve ser entre 1 e 1000."}).encode())
                    return
                db_set_setting("max_users", str(limit))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "max_users": limit}).encode())
                return
            except Exception as e:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
                return

        # Início de Scraping - VINCULADO AO USUÁRIO
        if parsed.path == "/api/start":
            data = json.loads(body_bytes.decode())
            keyword = data.get("keyword")
            city = data.get("city")
            depth = int(data.get("depth", 2))

            lat, lon = geocode_city(city)
            if not lat or not lon:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Coordenadas não encontradas para: '{city}'"}).encode())
                return

            job_name = f"{keyword} em {city}"
            payload = {
                "name": job_name,
                "keywords": [f"{keyword} em {city}"],
                "lat": lat,
                "lon": lon,
                "zoom": 15,
                "radius": 10000,
                "depth": depth,
                "email": False,
                "fast_mode": False,
                "lang": "pt",
                "max_time": 300,
            }

            try:
                raw = scraper_req("POST", "/api/v1/jobs", payload)
                resp_data = json.loads(raw)
                job_id = resp_data.get("id")

                # Grava no SQLite a relação: este job pertence exclusivamente a este user_id
                db_add_user_job(user["id"], job_id, job_name)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"id": job_id, "name": job_name}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Erro no scraper: {str(e)}"}).encode())
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        return

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

def run():
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    with ThreadedTCPServer(("", PORT), AuthHandler) as httpd:
        print(f"Painel Web com Autenticacao e Isolamento por Usuario iniciado!")
        print(f"Banco de dados SQLite: {DB_PATH}")
        print(f"Limite maximo: {MAX_USERS} usuarios")
        print(f"Acesse: http://localhost:{PORT}")
        sys.stdout.flush()
        httpd.serve_forever()

if __name__ == "__main__":
    run()
