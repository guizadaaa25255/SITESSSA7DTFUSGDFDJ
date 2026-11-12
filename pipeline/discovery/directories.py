# -*- coding: utf-8 -*-
"""
pipeline/discovery/directories.py
Coleta links de diretórios públicos de grupos brasileiros:
1. TopGruposBR (via API REST Supabase paginada)
2. Divulgrupo (40+ categorias e múltiplas páginas)
3. GruposDoTelegram (Busca Ajax e decodificação Base64 reversa)
4. WGrupos (Resolução de formulários por tags)
5. TeleGrupos.com.br (Varredura de posts)
6. TeleGruposBrasil & Metricgram
"""

import base64
import html
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Set, Tuple
import requests
from bs4 import BeautifulSoup

from pipeline.extract.extractor import canonicalize_url, extract_telegram_links

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
}


def fetch_topgruposbr() -> List[Tuple[str, str]]:
    """Coleta links do TopGruposBR direto da API pública do Supabase."""
    print("[Discovery] 📦 Coletando TopGruposBR (API Supabase)...")
    links: Set[str] = set()
    try:
        r = requests.get("https://topgruposbr.com/assets/index-B6gS3jMR.js", headers=HEADERS, timeout=10)
        key_m = re.search(r"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+", r.text)
        if not key_m:
            return []
        key = key_m.group(0)

        offset = 0
        limit = 1000
        while offset < 25000:
            h = {"apikey": key, "Authorization": f"Bearer {key}", "Range": f"{offset}-{offset + limit - 1}"}
            url = "https://kbubfcenmmydvxyqkayf.supabase.co/rest/v1/telegram_groups?select=telegram_url,name,description,members_count&is_active=eq.true"
            rq = requests.get(url, headers=h, timeout=12)
            if rq.status_code not in (200, 206):
                break
            items = rq.json()
            if not items:
                break
            for row in items:
                u = row.get("telegram_url")
                if u:
                    c = canonicalize_url(u)
                    if c:
                        links.add(c[0])
            if len(items) < limit:
                break
            offset += limit
    except Exception as e:
        print(f"[Discovery] Erro no TopGruposBR: {e}")

    print(f"[Discovery] ✔ TopGruposBR: {len(links)} links obtidos.")
    return [(l, "topgruposbr") for l in links]


def fetch_divulgrupo() -> List[Tuple[str, str]]:
    """Coleta links do Divulgrupo varrendo categorias."""
    print("[Discovery] 📂 Coletando Divulgrupo...")
    links: Set[str] = set()
    categorias = [
        "academia", "amizade", "amor-e-romance", "animais", "apostas", "artesanato",
        "carros-e-motos", "cidades-e-regioes", "ciencia", "compra-e-venda", "concursos",
        "cortes-de-videos", "criptomoedas", "culinaria-e-receitas", "cursos",
        "desenhos-e-animes", "divulgacao", "educacao", "investimentos", "esportes",
        "figurinhas-e-stickers", "filmes-e-series", "ganhar-dinheiro", "imobiliaria",
        "links", "memes-e-humor", "moda-e-beleza", "musicas", "namoro", "ofertas-e-cupons",
        "saude-e-bem-estar", "tecnologia"
    ]
    urls = []
    for cat in categorias:
        urls.append(f"https://divulgrupo.com.br/categoria/{cat}")
        urls.append(f"https://divulgrupo.com.br/categoria/{cat}/2")
        urls.append(f"https://divulgrupo.com.br/categoria/{cat}/3")

    session = requests.Session()
    session.headers.update(HEADERS)

    def fetch_url(u):
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                return extract_telegram_links(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = [ex.submit(fetch_url, u) for u in urls]
        for f in as_completed(futs):
            res = f.result()
            if res:
                links.update(res)

    print(f"[Discovery] ✔ Divulgrupo: {len(links)} links obtidos.")
    return [(l, "divulgrupo") for l in links]


def fetch_gruposdotelegram() -> List[Tuple[str, str]]:
    """Coleta links de gruposdotelegram.com.br via Ajax e decodificação reversa."""
    print("[Discovery] 🔐 Coletando GruposDoTelegram (Ajax + B64)...")
    links: Set[str] = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    urls_post: Set[str] = set()
    for cat_id in range(1, 36):
        for pag in range(1, 4):
            try:
                r = session.post(
                    "https://gruposdotelegram.com.br/ajax/buscar_posts.php",
                    data={"categoria": cat_id, "pagina": pag},
                    timeout=8
                )
                if r.status_code == 200:
                    found = re.findall(r'href=["\'](https://gruposdotelegram\.com\.br/grupo-[^"\']+)["\']', r.text)
                    urls_post.update(found)
            except Exception:
                pass

    def decode_post(u):
        try:
            rp = session.get(u, timeout=8)
            if rp.status_code == 200:
                m = re.search(r'var tg_k\s*=\s*["\']([^"\']+)["\']', rp.text)
                if m:
                    raw_b64 = m.group(1)
                    dec = base64.b64decode(raw_b64).decode("utf-8", errors="ignore")[::-1]
                    c = canonicalize_url(dec)
                    if c:
                        return c[0]
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=20) as ex:
        for res in ex.map(decode_post, urls_post):
            if res:
                links.add(res)

    print(f"[Discovery] ✔ GruposDoTelegram: {len(links)} links obtidos.")
    return [(l, "gruposdotelegram") for l in links]


def fetch_wgrupos() -> List[Tuple[str, str]]:
    """Coleta links do wgrupos.com/tag/telegram."""
    print("[Discovery] 🕷️ Coletando WGrupos...")
    links: Set[str] = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    tags = [
        "amizade", "apostas-desportivas", "ofertas", "tecnologia", "criptodivisas",
        "filmes", "gamers", "noticias", "compra-venda", "religiao", "livros",
        "desenhos-e-animes", "musica", "empregos", "educacao"
    ]
    all_ids: Set[str] = set()
    for tag in tags:
        for p in (1, 2, 3):
            url = f"https://www.wgrupos.com/tag/telegram/{tag}/{p}" if p > 1 else f"https://www.wgrupos.com/tag/telegram/{tag}"
            try:
                r = session.get(url, timeout=8)
                if r.status_code == 200:
                    ids = re.findall(r"lnkgroup\(['\"]?(\d+)['\"]?\)", r.text)
                    all_ids.update(ids)
            except Exception:
                pass

    target_post = "https://www.wgrupos.com/grupo/20261102"
    def resolve_w(gid):
        try:
            r = session.post(target_post, data={"id": gid, "data": "20261102"}, timeout=7)
            if r.status_code == 200:
                return extract_telegram_links(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = [ex.submit(resolve_w, gid) for gid in all_ids]
        for f in as_completed(futs):
            res = f.result()
            if res:
                links.update(res)

    print(f"[Discovery] ✔ WGrupos: {len(links)} links obtidos.")
    return [(l, "wgrupos") for l in links]


def fetch_telegrupos() -> List[Tuple[str, str]]:
    """Coleta links de telegrupos.com.br varrendo páginas e posts."""
    print("[Discovery] 📑 Coletando TeleGrupos.com.br...")
    links: Set[str] = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    post_urls = set()
    for p in range(1, 15):
        u = f"https://telegrupos.com.br/page/{p}/" if p > 1 else "https://telegrupos.com.br/"
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all("a", href=True):
                    h = a["href"].strip().split("?")[0].rstrip("/")
                    txt = a.text.strip().lower()
                    if ("entrar" in txt or len(h) > 28) and h.startswith("https://telegrupos.com.br/"):
                        if not any(x in h for x in ["/page/", "/categoria/", "/tag/", "minha-conta", "termos", "politica"]):
                            post_urls.add(h)
        except Exception:
            pass

    def fetch_post(u):
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                return extract_telegram_links(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = [ex.submit(fetch_post, u) for u in post_urls]
        for f in as_completed(futs):
            res = f.result()
            if res:
                links.update(res)

    print(f"[Discovery] ✔ TeleGrupos.com.br: {len(links)} links obtidos.")
    return [(l, "telegrupos_br") for l in links]


def fetch_outros_diretorios() -> List[Tuple[str, str]]:
    """Coleta links de hubs como TeleGruposBrasil e MetricGram."""
    print("[Discovery] 🌐 Coletando TeleGruposBrasil e MetricGram...")
    links: Set[str] = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    urls = [
        "https://telegruposbrasil.com/canais-de-cripto-telegram",
        "https://telegruposbrasil.com/canais-de-filmes-e-series-telegram",
        "https://telegruposbrasil.com/canais-de-jogos-telegram",
        "https://telegruposbrasil.com/canais-de-tecnologia-telegram",
        "https://telegruposbrasil.com/grupos-de-empregos-telegram",
        "https://telegruposbrasil.com/grupos-de-estudos-e-concursos-telegram",
        "https://telegruposbrasil.com/grupos-de-futebol-telegram",
        "https://telegruposbrasil.com/grupos-de-ofertas-e-promocoes-telegram",
        "https://telegruposbrasil.com/grupos-de-renda-extra-telegram",
        "https://metricgram.com/pt/tools/telegram-group-finder",
    ]

    for u in urls:
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                links.update(extract_telegram_links(r.text))
        except Exception:
            pass

    print(f"[Discovery] ✔ Outros diretórios: {len(links)} links obtidos.")
    return [(l, "outros_diretorios") for l in links]


def fetch_all_directories() -> List[Tuple[str, str]]:
    """Executa todos os coletores de diretórios públicos."""
    todos: List[Tuple[str, str]] = []
    todos.extend(fetch_topgruposbr())
    todos.extend(fetch_divulgrupo())
    todos.extend(fetch_gruposdotelegram())
    todos.extend(fetch_wgrupos())
    todos.extend(fetch_telegrupos())
    todos.extend(fetch_outros_diretorios())
    return todos
