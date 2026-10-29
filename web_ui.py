#!/usr/bin/env python3
"""Interface Web Local para o Google Maps Scraper Kit.
Roda 100% com a biblioteca padrão do Python (sem pip install).
Serve em http://localhost:5000
"""

import csv
import http.server
import io
import json
import os
import socketserver
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE_API = os.environ.get("SCRAPER_BASE_URL") or f"http://localhost:{os.environ.get('SCRAPER_PORT') or '8080'}"
UA = "google-maps-scraper-kit-web/1.0"
PORT = 5000

HTML_PAGE = """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Google Maps Scraper - Painel de Controle</title>
  <style>
    :root {
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --bg: #0f172a;
      --surface: #1e293b;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --success: #10b981;
      --warning: #f59e0b;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    body { background: var(--bg); color: var(--text); padding: 2rem; min-height: 100vh; }
    .container { max-width: 1100px; margin: 0 auto; }
    header { margin-bottom: 2rem; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 1.5rem; }
    h1 { font-size: 1.8rem; display: flex; align-items: center; gap: 0.75rem; }
    .badge { font-size: 0.8rem; background: #064e3b; color: #34d399; padding: 0.25rem 0.6rem; border-radius: 999px; font-weight: 600; }
    .layout { display: grid; grid-template-columns: 360px 1fr; gap: 2rem; }
    @media (max-width: 850px) { .layout { grid-template-columns: 1fr; } }
    .card { background: var(--surface); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1.5rem; }
    .card h2 { font-size: 1.2rem; margin-bottom: 1.25rem; color: var(--text); }
    .form-group { margin-bottom: 1.25rem; }
    label { display: block; font-size: 0.875rem; font-weight: 500; margin-bottom: 0.5rem; color: var(--text-muted); }
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
      <h1>📍 Google Maps Scraper <span class="badge">Pronto</span></h1>
      <span style="font-size: 0.85rem; color: var(--text-muted);">API local conectada na porta 8080</span>
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
            <div class="help-text">1 a 3 para testes rápidos; 5 a 10 para buscas completas.</div>
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
        <div style="display: flex; justify-content: space-between; align-items: center;">
          <h2>Tarefas e Resultados</h2>
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
              <tr><td colspan="4" style="text-align: center; color: var(--text-muted); padding: 2rem;">Carregando...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    let activePollInterval = null;

    async function loadJobs() {
      try {
        const res = await fetch('/api/jobs');
        const jobs = await res.json();
        const tbody = document.getElementById('jobs-tbody');
        if (!jobs || jobs.length === 0) {
          tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: var(--text-muted); padding: 2rem;">Nenhuma busca realizada ainda.</td></tr>';
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
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'Falha ao iniciar busca');

        statusText.innerHTML = `Busca <strong>${data.name}</strong> em andamento! Acompanhe na tabela ao lado.`;
        loadJobs();

        // Polling do job
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

    // Carrega a lista ao abrir a tela
    loadJobs();
    setInterval(loadJobs, 10000);
  </script>
</body>
</html>
"""

def geocode_city(place):
    """Consulta OpenStreetMap Nominatim para pegar latitude e longitude"""
    q = urllib.parse.urlencode({"format": "json", "limit": 1, "q": place})
    url = f"https://nominatim.openstreetmap.org/search?{q}"
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=15) as resp:
        hits = json.loads(resp.read().decode())
        if hits:
            return str(hits[0]["lat"]), str(hits[0]["lon"])
    return None, None

def scraper_req(method, path, body=None):
    """Encaminha requisição para a engine local gosom na porta 8080"""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", "User-Agent": UA}
    req = urllib.request.Request(BASE_API + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
            return

        elif parsed.path == "/api/jobs":
            try:
                raw = scraper_req("GET", "/api/v1/jobs")
                jobs = json.loads(raw) or []
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(jobs).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return

        elif parsed.path == "/api/job":
            qs = urllib.parse.parse_qs(parsed.query)
            job_id = qs.get("id", [""])[0]
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
        if parsed.path == "/api/start":
            content_len = int(self.headers.get("Content-Length", 0))
            body_bytes = self.rfile.read(content_len)
            data = json.loads(body_bytes.decode())

            keyword = data.get("keyword")
            city = data.get("city")
            depth = int(data.get("depth", 2))

            # 1. Geocodificar
            lat, lon = geocode_city(city)
            if not lat or not lon:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": f"Não foi possível encontrar as coordenadas para a cidade: '{city}'"}).encode())
                return

            # 2. Criar Job no Gosom Scraper
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
        # Silencia logs de requisições repetidas para não poluir
        return

def run():
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Painel Web iniciado com sucesso!")
        print(f"Abra no seu navegador: http://localhost:{PORT}")
        sys.stdout.flush()
        httpd.serve_forever()

if __name__ == "__main__":
    run()
