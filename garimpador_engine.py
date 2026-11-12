# -*- coding: utf-8 -*-
"""
garimpador_engine.py
====================
Motor de garimpo exaustivo para extrair TODOS os grupos de QUALQUER site.

Funcionalidades:
- Percorre TODAS as páginas de paginação do site (/page/1..N, /telegram/1..N, ?page=N, ?p=N)
- Suporte dedicado aos maiores portais BR:
  * WGrupos.com (resgate de links via formulário/redirecionamento lnkgroup)
  * TeleGrupos.com.br (varredura de todas as categorias, páginas de post e extração do botão "ENTRAR NO GRUPO" com bypass anti-WAF)
  * GruposDoTelegram.com.br (decodificação de tokens base64 invertidos via Ajax e páginas)
  * TopGruposBR.com (acesso direto via Supabase REST API)
  * Sites genéricos / blogs WordPress / diretórios de links
- Extração precisa de URLs do Telegram (t.me, telegram.me, telegram.dog, tg://join, tg://resolve)
- Descarte automático de bots internos do site (PlusListasBot, TeleGruposBot, share, etc.)
- Controle de concorrência com sessões HTTP para não ser bloqueado por WAF/MalCare
"""

import base64
import html
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}

RE_TG_RAW = re.compile(
    r"(?:https?://(?:t|telegram)\.me/[a-zA-Z0-9_+\-]+(?:/[0-9]+)?|"
    r"https?://telegram\.dog/[a-zA-Z0-9_+\-]+|"
    r"tg://(?:resolve\?domain=|join\?invite=)[a-zA-Z0-9_+\-]+)",
    re.I
)

# Termos / bots que NÃO são grupos reais de usuários
IGNORAR_USERNAMES = {
    "share", "telegram", "whatsapptelegramgrupos", "telegrupos18bot",
    "listatelegruposbot", "pluslistasbot", "suporte", "telegramorg",
    "addstickers", "iv", "socks", "proxy", "login", "signup"
}


def clean_tg_link(raw_url: str) -> str:
    """Valida e normaliza o link do Telegram, descartando bots do site e lixo."""
    if not raw_url:
        return ""
    u = raw_url.strip()

    # Esquemas tg://
    if u.lower().startswith("tg://resolve?domain="):
        u = f"https://t.me/{u.split('=')[-1]}"
    elif u.lower().startswith("tg://join?invite="):
        u = f"https://t.me/+{u.split('=')[-1]}"

    u = u.replace("http://", "https://")
    u = u.replace("https://telegram.me/", "https://t.me/")
    u = u.replace("https://telegram.dog/", "https://t.me/")
    u = u.split("?")[0].rstrip("/")

    # Garantir que é um domínio t.me legítimo
    if not u.startswith("https://t.me/"):
        return ""

    partes = u.split("/")
    if len(partes) < 4:
        return ""
    user = partes[3].lower()

    if user in IGNORAR_USERNAMES:
        return ""
    if user.endswith("bot") and user not in ("grupocba", "robot"):
        # Descarta bots de divulgação automática
        return ""
    if u == "https://t.me/+4PuhVpk3N-IyYjYx":  # canal de avisos do telegrupos
        return ""
    if len(user) < 3:
        return ""
    if u.startswith("https://t.me/c/"):
        return ""

    return u


def extrair_links_tg_do_texto(texto_html: str) -> set:
    """Extrai todos os links do Telegram de uma string HTML decodificada."""
    if not texto_html:
        return set()
    txt = html.unescape(texto_html)
    try:
        txt_unq = urllib.parse.unquote(txt)
    except Exception:
        txt_unq = ""
    busca = txt + "\n" + txt_unq

    encontrados = set()
    for m in RE_TG_RAW.findall(busca):
        link = clean_tg_link(m)
        if link:
            encontrados.add(link)
    return encontrados


def _garimpar_wgrupos(session, url_raiz, max_paginas, notificar):
    """Varre wgrupos.com percorrendo as páginas e resolvendo os grupos via formulário."""
    encontrados = set()
    form_action = "/grupo/20261103"
    form_data = "20261103"
    all_ids = set()

    limite = min(max_paginas, 120)
    notificar(f"WGrupos: Percorrendo até {limite} páginas de grupos...")

    for p in range(1, limite + 1):
        p_url = f"https://www.wgrupos.com/telegram/{p}" if p > 1 else url_raiz
        try:
            r = session.get(p_url, timeout=10)
            if r.status_code != 200:
                break
            soup = BeautifulSoup(r.text, "html.parser")
            form = soup.find("form", id="formlnk")
            if form:
                form_action = form.get("action") or form_action
                inp_data = form.find("input", id="data") or form.find("input", {"name": "data"})
                if inp_data:
                    form_data = inp_data.get("value", form_data)
            ids = re.findall(r"lnkgroup\(['\"]?(\d+)['\"]?\)", r.text)
            if not ids:
                break  # Fim da paginação
            all_ids.update(ids)
            if p % 5 == 0 or p == 1:
                notificar(f"WGrupos: Página {p} analisada ({len(all_ids)} grupos encontrados até agora)...", pags=p)
        except Exception:
            pass

    target_post_url = "https://www.wgrupos.com/" + form_action.lstrip("/")
    notificar(f"WGrupos: Resolvendo {len(all_ids)} grupos em paralelo...")

    def resolver_w(gid):
        try:
            r = session.post(target_post_url, data={"id": gid, "data": form_data}, timeout=8)
            if r.status_code == 200:
                return extrair_links_tg_do_texto(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(resolver_w, gid): gid for gid in all_ids}
        for f in as_completed(futs):
            res = f.result()
            if res:
                encontrados.update(res)
                if len(encontrados) % 50 == 0:
                    notificar(f"WGrupos: {len(encontrados)} links válidos do Telegram extraídos...")

    return encontrados


def _garimpar_telegrupos(session, url_raiz, max_paginas, notificar):
    """Varre telegrupos.com.br percorrendo todas as categorias e páginas de posts com URL trailing slash obrigatório."""
    encontrados = set()
    post_urls = set()

    # Garantir trailing slash na URL raiz
    if not url_raiz.endswith("/"):
        url_raiz += "/"

    cat_urls = []
    if "/categoria/" in url_raiz:
        cat_urls = [url_raiz]
    else:
        try:
            r_home = session.get(url_raiz, timeout=10)
            if r_home.status_code == 200:
                soup_home = BeautifulSoup(r_home.text, "html.parser")
                for a in soup_home.find_all("a", href=True):
                    h = a["href"].strip()
                    if "/categoria/grupos-telegram-" in h or "/categoria/" in h:
                        h_clean = h.split("?")[0].split("#")[0]
                        if not h_clean.endswith("/"):
                            h_clean += "/"
                        if h_clean not in cat_urls and h_clean.startswith("https://telegrupos.com.br/"):
                            cat_urls.append(h_clean)
        except Exception:
            pass

    if not cat_urls:
        cat_urls = [url_raiz]

    notificar(f"TeleGrupos: Mapeando {len(cat_urls)} seções e todas as suas páginas...")

    # Percorrer cada categoria e todas as páginas de paginação
    limite_pags_por_cat = 25 if len(cat_urls) > 5 else min(max_paginas, 60)

    for cat_idx, cat in enumerate(cat_urls):
        for p in range(1, limite_pags_por_cat + 1):
            p_url = f"{cat}page/{p}/" if p > 1 else cat
            try:
                r = session.get(p_url, timeout=9)
                if r.status_code != 200:
                    break
                soup = BeautifulSoup(r.text, "html.parser")
                novos = 0
                for a in soup.find_all("a", href=True):
                    h = a["href"].strip()
                    txt = a.text.strip().upper()
                    # Botão ENTRAR ou links de artigos de grupos
                    if (txt == "ENTRAR" or "ENTRAR" in txt or len(h) > 28) and h.startswith("https://telegrupos.com.br/"):
                        h_clean = h.split("?")[0].split("#")[0]
                        if not h_clean.endswith("/"):
                            h_clean += "/"
                        if not any(x in h_clean for x in ["/page/", "/categoria/", "/tag/", "minha-conta", "termos", "politica", "impulsionar"]):
                            if h_clean not in post_urls:
                                post_urls.add(h_clean)
                                novos += 1
                if novos == 0 and p > 1:
                    break
            except Exception:
                pass

        if (cat_idx + 1) % 5 == 0 or cat_idx == len(cat_urls) - 1:
            notificar(f"TeleGrupos: {len(post_urls)} posts de grupos mapeados ({cat_idx + 1}/{len(cat_urls)} seções)...", pags=len(post_urls))

    notificar(f"TeleGrupos: Extraindo links do Telegram de {len(post_urls)} posts (com preservação de trailing slash)...")

    def fetch_post(u):
        time.sleep(0.05)  # delay educado
        try:
            r = session.get(u, timeout=9)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                links = set()
                # 1. Botão 'ENTRAR NO GRUPO'
                for a in soup.find_all("a", href=True):
                    h = a["href"]
                    txt = a.text.strip().lower()
                    if ("entrar" in txt or "acessar" in txt) and ("t.me" in h or "telegram.me" in h):
                        c = clean_tg_link(h)
                        if c:
                            links.add(c)
                # 2. Varredura no texto
                if not links:
                    for m in re.findall(r"https?://(?:t|telegram)\.me/[a-zA-Z0-9_\+\-]+", r.text):
                        c = clean_tg_link(m)
                        if c:
                            links.add(c)
                return links
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(fetch_post, u): u for u in post_urls}
        for f in as_completed(futs):
            res = f.result()
            if res:
                encontrados.update(res)
                if len(encontrados) % 25 == 0:
                    notificar(f"TeleGrupos: {len(encontrados)} grupos extraídos com sucesso...")

    return encontrados


def _garimpar_gruposdotelegram(session, url_raiz, max_paginas, notificar):
    """Varre gruposdotelegram.com.br percorrendo todas as categorias e decodificando tokens base64."""
    encontrados = set()
    notificar("GruposDoTelegram: Consultando categorias e decodificando links...")

    urls_post = set()
    # 35 categorias com paginação
    for cat_id in range(1, 36):
        for pag in range(1, 6):
            try:
                r = session.post(
                    "https://gruposdotelegram.com.br/ajax/buscar_posts.php",
                    data={"categoria": cat_id, "pagina": pag},
                    timeout=8
                )
                if r.status_code == 200:
                    found = re.findall(r'href=["\'](https://gruposdotelegram\.com\.br/grupo-[^"\']+)["\']', r.text)
                    if not found:
                        break
                    urls_post.update(found)
            except Exception:
                pass

    notificar(f"GruposDoTelegram: {len(urls_post)} posts encontrados. Decodificando links em paralelo...")

    def decodificar_post(u):
        try:
            rp = session.get(u, timeout=8)
            if rp.status_code == 200:
                m = re.search(r'var tg_k\s*=\s*["\']([^"\']+)["\']', rp.text)
                if m:
                    raw_b64 = m.group(1)
                    dec = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")[::-1]
                    c = clean_tg_link(dec)
                    if c:
                        return c
                for mt in re.findall(r"https?://(?:t|telegram)\.me/[a-zA-Z0-9_\+\-]+", rp.text):
                    c = clean_tg_link(mt)
                    if c:
                        return c
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(decodificar_post, u): u for u in urls_post}
        for f in as_completed(futs):
            res = f.result()
            if res:
                encontrados.add(res)
                if len(encontrados) % 25 == 0:
                    notificar(f"GruposDoTelegram: {len(encontrados)} links decodificados...")

    return encontrados


def _garimpar_topgruposbr(session, notificar):
    """Varre topgruposbr.com diretamente pela API Supabase."""
    encontrados = set()
    notificar("TopGruposBR: Acessando base de dados Supabase...")
    api_url = "https://kbubfcenmmydvxyqkayf.supabase.co/rest/v1/grupos?select=link_telegram,titulo,descricao"
    headers_supa = {
        "apikey": (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
            "eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtidWJmY2VubW15ZHZ4eXFrYXlmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzgyOTQ0MzUsImV4cCI6MjA1Mzg3MDQzNX0."
            "4j-yGv3i7i3n6r0mS3yU3fT4WvE0b1l2X7z"
        ),
        "Range": "0-999"
    }

    for offset in range(0, 4000, 1000):
        try:
            h = headers_supa.copy()
            h["Range"] = f"{offset}-{offset+999}"
            r = session.get(api_url, headers=h, timeout=12)
            if r.status_code in (200, 206):
                data = r.json()
                if not data:
                    break
                for item in data:
                    lk = clean_tg_link(item.get("link_telegram") or "")
                    if lk:
                        encontrados.add(lk)
                notificar(f"TopGruposBR: {len(encontrados)} grupos obtidos do Supabase...")
        except Exception:
            pass

    return encontrados


def _garimpar_generico(session, url_raiz, subpages, profundo, max_paginas, notificar):
    """Varre qualquer outro site com busca em largura e paginação profunda."""
    encontrados = set()
    dominio = urllib.parse.urlparse(url_raiz).netloc.lower().replace("www.", "")
    visitados = set()
    fila = [url_raiz]

    limite = 500 if profundo else min(max_paginas, 150)

    while fila and len(visitados) < limite:
        u = fila.pop(0)
        if u in visitados:
            continue
        visitados.add(u)

        try:
            r = session.get(u, timeout=10)
            if r.status_code != 200:
                continue

            links_pagina = extrair_links_tg_do_texto(r.text)
            if links_pagina:
                encontrados.update(links_pagina)

            if not subpages:
                continue

            soup = BeautifulSoup(r.text, "html.parser")
            novas_urls = []
            for a in soup.find_all("a", href=True):
                h = a["href"].strip()
                if not h or h.startswith("#") or h.startswith("javascript:"):
                    continue
                abs_u = urllib.parse.urljoin(u, h).split("#")[0].rstrip("/")
                if not abs_u.startswith("http"):
                    continue
                d_abs = urllib.parse.urlparse(abs_u).netloc.lower().replace("www.", "")
                if d_abs == dominio and abs_u not in visitados and abs_u not in fila:
                    if not any(abs_u.lower().endswith(ext) for ext in [".css", ".js", ".png", ".jpg", ".jpeg", ".webp", ".svg", ".ico", ".pdf"]):
                        novas_urls.append(abs_u)

            pags = [x for x in novas_urls if any(k in x.lower() for k in ["page", "pag", "?p="])]
            outros = [x for x in novas_urls if x not in pags]
            fila = pags + fila + outros

            if len(visitados) % 10 == 0:
                notificar(f"Vasculhando {dominio}: {len(visitados)} págs · {len(encontrados)} grupos encontrados...")

        except Exception:
            pass

    return encontrados


def garimpar_site_completo(
    urls_iniciais: list,
    subpages: bool = True,
    profundo: bool = False,
    max_paginas: int = 500,
    progress_callback=None
) -> set:
    """Função principal: despacha para handlers dedicados ou genérico."""
    session = requests.Session()
    session.headers.update(HEADERS)

    todos_links = set()
    total_paginas = [0]

    def notificar(msg, pags=None):
        if pags is not None:
            total_paginas[0] = max(total_paginas[0], pags)
        if progress_callback:
            progress_callback({
                "paginas": total_paginas[0],
                "links_encontrados": len(todos_links),
                "mensagem": msg
            })

    limite = 200 if profundo else 100

    for url in urls_iniciais:
        url = url.strip()
        if not url.startswith("http"):
            continue

        dominio = urllib.parse.urlparse(url).netloc.lower().replace("www.", "")
        notificar(f"Iniciando garimpo em {dominio}...")

        if "wgrupos.com" in dominio:
            links = _garimpar_wgrupos(session, url, limite, notificar)
        elif "telegrupos.com.br" in dominio:
            links = _garimpar_telegrupos(session, url, limite, notificar)
        elif "gruposdotelegram.com.br" in dominio:
            links = _garimpar_gruposdotelegram(session, url, limite, notificar)
        elif "topgruposbr.com" in dominio:
            links = _garimpar_topgruposbr(session, notificar)
        else:
            links = _garimpar_generico(session, url, subpages, profundo, max_paginas, notificar)

        todos_links.update(links)
        notificar(f"Finalizado {dominio}: {len(links)} links extraídos.")

    notificar(f"Garimpo completo! Total extraído: {len(todos_links)} links.")
    return todos_links
