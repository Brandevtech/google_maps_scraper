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
PORT = 5000
MAX_USERS = 3
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")

# ── Banco de Dados SQLite ─────────────────────────────────────────────────────
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
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
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Login - Google Maps Scraper</title>
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <style>
    :root {
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --bg: #0b1220;
      --surface: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --danger: #ef4444;
      --success: #10b981;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); display: flex; align-items: center; justify-content: center; min-height: 100vh; padding: 1.5rem; }
    .card { background: var(--surface); border: 1px solid var(--border); border-radius: 1rem; width: 100%; max-width: 440px; padding: 2.2rem; box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.4); text-align: center; }
    .logo { font-size: 2.5rem; margin-bottom: 0.5rem; }
    h1 { font-size: 1.5rem; font-weight: 700; margin-bottom: 0.5rem; }
    .subtitle { color: var(--text-muted); font-size: 0.88rem; margin-bottom: 1.75rem; line-height: 1.4; }
    
    .quota-box { background: rgba(15, 23, 42, 0.8); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1rem; margin-bottom: 1.75rem; text-align: left; }
    .quota-header { display: flex; justify-content: space-between; font-size: 0.8rem; font-weight: 600; margin-bottom: 0.5rem; }
    .quota-bar-bg { width: 100%; height: 8px; background: #334155; border-radius: 999px; overflow: hidden; }
    .quota-bar-fill { height: 100%; background: var(--primary); transition: width 0.3s; }
    .quota-bar-fill.full { background: var(--danger); }
    .quota-desc { font-size: 0.75rem; color: var(--text-muted); margin-top: 0.5rem; }

    .btn-google-container { display: flex; justify-content: center; margin-bottom: 1rem; min-height: 44px; }
    

    .alert { padding: 0.75rem 1rem; border-radius: 0.5rem; font-size: 0.82rem; margin-top: 1rem; text-align: left; display: none; }
    .alert-danger { background: rgba(239, 68, 68, 0.15); border: 1px solid var(--danger); color: #fca5a5; display: block; }
    .alert-success { background: rgba(16, 185, 129, 0.15); border: 1px solid var(--success); color: #86efac; display: block; }
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">📍</div>
    <h1>Acesso ao Scraper</h1>
    <p class="subtitle">Faça login com a sua conta Google para acessar o seu painel de leads exclusivo.</p>

    <!-- Indicador de Vagas de Usuários (Limite de 3) -->
    <div class="quota-box">
      <div class="quota-header">
        <span>Vagas de Usuários</span>
        <span id="quota-text">Carregando...</span>
      </div>
      <div class="quota-bar-bg">
        <div id="quota-fill" class="quota-bar-fill" style="width: 0%;"></div>
      </div>
      <p id="quota-desc" class="quota-desc">Limite máximo de 3 usuários autorizados.</p>
    </div>

    <!-- Botão Oficial do Google com Redirect Nativo (Zero erro de COOP / popup) -->
    <div id="google-btn-section">
      <div id="g_id_onload"
           data-client_id="{{GOOGLE_CLIENT_ID}}"
           data-context="signin"
           data-ux_mode="redirect"
           data-login_uri="/login"
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
    async function updateQuota() {
      try {
        const res = await fetch('/api/auth/status');
        const data = await res.json();
        const count = data.user_count;
        const max = data.max_users;
        const pct = Math.min(100, Math.round((count / max) * 100));

        document.getElementById('quota-text').textContent = `${count} de ${max} cadastrados`;
        const fill = document.getElementById('quota-fill');
        fill.style.width = pct + '%';
        if (count >= max) {
          fill.classList.add('full');
          document.getElementById('quota-desc').innerHTML = '⚠️ <strong>Limite atingido.</strong> Apenas os 3 usuários já cadastrados podem entrar.';
        } else {
          document.getElementById('quota-desc').textContent = `${max - count} vaga(s) restante(s). Novos usuários serão cadastrados automaticamente.`;
        }
      } catch (e) {
        console.error(e);
      }
    }

    

    function showAlert(msg, type) {
      const box = document.getElementById('alert-box');
      box.textContent = msg;
      box.className = 'alert alert-' + type;
      box.style.display = 'block';
    }

    updateQuota();
  </script>
</body>
</html>
"""

APP_PAGE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Google Maps Scraper - Painel</title>
  <style>
    :root {
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --bg: #0b1220;
      --surface: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 2rem; min-height: 100vh; }
    .container { max-width: 1100px; margin: 0 auto; }
    
    header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 1.25rem; flex-wrap: wrap; gap: 1rem; }
    .header-left { display: flex; align-items: center; gap: 0.75rem; }
    h1 { font-size: 1.6rem; display: flex; align-items: center; gap: 0.5rem; }
    .badge { font-size: 0.75rem; background: #064e3b; color: #34d399; padding: 0.2rem 0.5rem; border-radius: 999px; font-weight: 600; }
    
    .user-profile { display: flex; align-items: center; gap: 0.75rem; background: rgba(30, 41, 59, 0.7); padding: 0.4rem 0.75rem; border-radius: 999px; border: 1px solid var(--border); }
    .user-avatar { width: 32px; height: 32px; border-radius: 50%; object-fit: cover; background: #3b82f6; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 0.85rem; color: #fff; }
    .user-info { font-size: 0.8rem; line-height: 1.2; }
    .user-name { font-weight: 600; color: #fff; }
    .user-email { color: var(--text-muted); font-size: 0.72rem; }
    .btn-logout { background: none; border: none; color: #f87171; cursor: pointer; font-size: 0.8rem; font-weight: 600; margin-left: 0.5rem; padding: 0.2rem 0.4rem; border-radius: 0.3rem; transition: 0.2s; }
    .btn-logout:hover { background: rgba(239, 68, 68, 0.15); }

    .layout { display: grid; grid-template-columns: 360px 1fr; gap: 2rem; }
    @media (max-width: 850px) { .layout { grid-template-columns: 1fr; } }
    
    .card { background: var(--surface); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1.5rem; }
    .card h2 { font-size: 1.15rem; margin-bottom: 1.25rem; color: var(--text); }
    .form-group { margin-bottom: 1.25rem; }
    label { display: block; font-size: 0.85rem; font-weight: 500; margin-bottom: 0.45rem; color: var(--text-muted); }
    input[type="text"], input[type="number"] { width: 100%; padding: 0.65rem 0.85rem; background: #0f172a; border: 1px solid var(--border); border-radius: 0.5rem; color: #fff; font-size: 0.95rem; }
    input:focus { outline: none; border-color: var(--primary); }
    .help-text { font-size: 0.75rem; color: var(--text-muted); margin-top: 0.35rem; }
    
    button.btn { width: 100%; padding: 0.75rem; background: var(--primary); color: white; border: none; border-radius: 0.5rem; font-weight: 600; cursor: pointer; transition: 0.2s; display: flex; align-items: center; justify-content: center; gap: 0.5rem; }
    button.btn:hover { background: var(--primary-hover); }
    button.btn:disabled { opacity: 0.5; cursor: not-allowed; }
    
    .status-box { margin-top: 1.25rem; padding: 0.75rem; border-radius: 0.5rem; font-size: 0.85rem; display: none; }
    .status-box.active { display: block; background: #1e1b4b; border: 1px solid #4338ca; color: #c7d2fe; }
    .status-box.done { display: block; background: #064e3b; border: 1px solid #059669; color: #a7f3d0; }
    .status-box.error { display: block; background: #450a0a; border: 1px solid #dc2626; color: #fecaca; }
    
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; font-size: 0.875rem; }
    th, td { text-align: left; padding: 0.75rem; border-bottom: 1px solid var(--border); }
    th { color: var(--text-muted); font-weight: 600; background: #0f172a; }
    tr:hover { background: rgba(255,255,255,0.02); }
    
    .status-pill { padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.75rem; font-weight: 600; }
    .pill-ok { background: #064e3b; color: #34d399; }
    .pill-working { background: #78350f; color: #fbbf24; }
    .pill-failed { background: #7f1d1d; color: #f87171; }
    
    .btn-download { display: inline-block; padding: 0.35rem 0.75rem; background: #059669; color: white; border-radius: 0.35rem; text-decoration: none; font-size: 0.75rem; font-weight: 600; }
    .btn-download:hover { background: #047857; }
    
    .spinner { border: 2px solid rgba(255,255,255,0.2); border-left-color: #fff; border-radius: 50%; width: 16px; height: 16px; animation: spin 0.8s linear infinite; display: inline-block; }
    @keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="header-left">
        <h1>📍 Google Maps Scraper</h1>
        <span class="badge">Sua Área Privada</span>
      </div>

      <div class="user-profile">
        {{USER_AVATAR}}
        <div class="user-info">
          <div class="user-name">{{USER_NAME}}</div>
          <div class="user-email">{{USER_EMAIL}}</div>
        </div>
        <button onclick="logout()" class="btn-logout" title="Sair do painel">Sair 🚪</button>
      </div>
    </header>

    <div class="layout">
      <!-- FORMULÁRIO -->
      <div class="card">
        <h2>Nova Busca</h2>
        <form id="scrape-form" onsubmit="startScrape(event)">
          <div class="form-group">
            <label for="keyword">O que buscar? (Palavra-chave)</label>
            <input type="text" id="keyword" required placeholder="Ex: padarias, dentistas, academias">
          </div>

          <div class="form-group">
            <label for="city">Cidade e Estado</label>
            <input type="text" id="city" required placeholder="Ex: Curitiba, PR ou São Paulo, SP">
            <div class="help-text">✨ Geocodificação automática por nome (sem latitude manual).</div>
          </div>

          <div class="form-group">
            <label for="depth">Profundidade (Qtd. de páginas/rolagens)</label>
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

      <!-- TABELA DE RESULTADOS EXCLUSIVA DO USUÁRIO -->
      <div class="card">
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <h2>Suas Buscas Realizadas</h2>
          <button onclick="loadJobs()" style="background: none; border: 1px solid var(--border); color: var(--text-muted); padding: 0.35rem 0.75rem; border-radius: 0.4rem; cursor: pointer; font-size: 0.8rem;">Atualizar Lista</button>
        </div>
        <div style="overflow-x: auto;">
          <table>
            <thead>
              <tr>
                <th>Nome / Busca</th>
                <th>Data</th>
                <th>Status</th>
                <th>Ações</th>
              </tr>
            </thead>
            <tbody id="jobs-tbody">
              <tr><td colspan="4" style="text-align: center; color: var(--text-muted); padding: 2rem;">Carregando suas buscas...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let activePollInterval = null;

    async function logout() {
      if (!confirm("Deseja realmente sair?")) return;
      await fetch('/api/auth/logout', { method: 'POST' });
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
            actions = `<span style="color: #f87171; font-size: 0.8rem;">Erro</span>`;
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
        console.error('Erro ao carregar jobs:', err);
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
  </script>
</body>
</html>
"""

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
        if db_get_user_count() >= MAX_USERS:
            return None, None, "Limite máximo de 3 usuários atingido no sistema. Novos cadastros estão bloqueados."
        user = db_create_user(google_id, email, name, picture)

    session_token = db_create_session(user["id"], days=30)
    return user, session_token, None

# ── Servidor HTTP ─────────────────────────────────────────────────────────────
class AuthHandler(http.server.BaseHTTPRequestHandler):
    def get_session_user(self):
        cookie_header = self.headers.get("Cookie", "")
        if not cookie_header:
            return None
        cookies = http.cookies.SimpleCookie(cookie_header)
        session_token = cookies.get("session_id")
        if not session_token:
            return None
        return db_get_user_by_session(session_token.value)

    def send_session_cookie(self, token, max_age=2592000): # 30 dias
        cookie = f"session_id={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}"
        self.send_header("Set-Cookie", cookie)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
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
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
            return

        # Status da cota de usuários
        elif parsed.path == "/api/auth/status":
            count = db_get_user_count()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({
                "user_count": count,
                "max_users": MAX_USERS,
                "is_full": count >= MAX_USERS,
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
            html = APP_PAGE.replace("{{USER_NAME}}", user["name"]).replace("{{USER_EMAIL}}", user["email"]).replace("{{USER_AVATAR}}", avatar_html)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
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
            if cookie_header:
                cookies = http.cookies.SimpleCookie(cookie_header)
                session_token = cookies.get("session_id")
                if session_token:
                    db_delete_session(session_token.value)
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

def run():
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), AuthHandler) as httpd:
        print(f"Painel Web com Autenticacao e Isolamento por Usuario iniciado!")
        print(f"Banco de dados SQLite: {DB_PATH}")
        print(f"Limite maximo: {MAX_USERS} usuarios")
        print(f"Acesse: http://localhost:{PORT}")
        sys.stdout.flush()
        httpd.serve_forever()

if __name__ == "__main__":
    run()
