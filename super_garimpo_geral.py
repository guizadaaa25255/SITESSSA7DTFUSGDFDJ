# -*- coding: utf-8 -*-
"""
super_garimpo_geral.py
======================
Script mestre para garimpar exaustivamente todos os grupos dos 7 sites fornecidos pelo usuário:
1. telegrupos.com.br (58 categorias)
2. gruposdotelegram.com.br (35 categorias)
3. wgrupos.com/telegram (20 categorias/tags)
4. telegruposbrasil.com (20 categorias + hubs)
5. metricgram.com (12 categorias)
6. topgruposbr.com (37 categorias)
7. divulgrupo.com.br (41 categorias)

Aplica o filtro estrito:
- Sem expirados
- Sem canais (só grupos)
- Sem repetidos
- Sem grupos onde não dá para mandar mensagem
- Só brasileiros (100% BR)
"""

import base64
import html
import json
import os
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
}

RE_TG_RAW = re.compile(
    r"(?:https?://(?:t|telegram)\.me/[a-zA-Z0-9_+\-]+(?:/[0-9]+)?|"
    r"https?://telegram\.dog/[a-zA-Z0-9_+\-]+|"
    r"tg://(?:resolve\?domain=|join\?invite=)[a-zA-Z0-9_+\-]+)",
    re.I
)

IGNORAR_USERNAMES = {
    "share", "telegram", "whatsapptelegramgrupos", "telegrupos18bot",
    "listatelegruposbot", "pluslistasbot", "suporte", "telegramorg",
    "parceriatelegrabot", "metricgramofficial", "addstickers", "iv",
    "socks", "proxy", "login", "signup"
}


def clean_tg(raw_url: str) -> str:
    if not raw_url:
        return ""
    u = raw_url.strip()
    if u.lower().startswith("tg://resolve?domain="):
        u = f"https://t.me/{u.split('=')[-1]}"
    elif u.lower().startswith("tg://join?invite="):
        u = f"https://t.me/+{u.split('=')[-1]}"
    u = u.replace("http://", "https://")
    u = u.replace("https://telegram.me/", "https://t.me/")
    u = u.replace("https://telegram.dog/", "https://t.me/")
    u = u.split("?")[0].rstrip("/")
    partes = u.split("/")
    if len(partes) < 4:
        return ""
    user = partes[3].lower()
    if user in IGNORAR_USERNAMES or len(user) < 3 or u.startswith("https://t.me/c/"):
        return ""
    if user.endswith("bot") and user not in ("grupocba", "robot"):
        return ""
    return u


def extrair_tme(texto: str) -> set:
    if not texto:
        return set()
    txt = html.unescape(texto)
    try:
        txt_unq = urllib.parse.unquote(txt)
    except Exception:
        txt_unq = ""
    res = set()
    for m in RE_TG_RAW.findall(txt + "\n" + txt_unq):
        c = clean_tg(m)
        if c:
            res.add(c)
    return res


# ─── 1. TOPGRUPOSBR (API Direta Supabase) ────────────────────────────────────
def coletar_topgruposbr() -> set:
    print("\n[1/7] 📦 Coletando TopGruposBR (API Supabase)...")
    links = set()
    try:
        r = requests.get('https://topgruposbr.com/assets/index-B6gS3jMR.js', headers=HEADERS, timeout=10)
        key_m = re.search(r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+', r.text)
        if not key_m:
            print("  Falha ao obter chave do Supabase.")
            return links
        key = key_m.group(0)
        
        # Paginar pelo Supabase
        offset = 0
        limit = 1000
        while offset < 3000:
            headers = {'apikey': key, 'Authorization': f'Bearer {key}', 'Range': f'{offset}-{offset + limit - 1}'}
            url = f'https://kbubfcenmmydvxyqkayf.supabase.co/rest/v1/telegram_groups?select=telegram_url&is_active=eq.true'
            rq = requests.get(url, headers=headers, timeout=12)
            if rq.status_code not in (200, 206):
                break
            items = rq.json()
            if not items:
                break
            for row in items:
                u = clean_tg(row.get('telegram_url', ''))
                if u:
                    links.add(u)
            if len(items) < limit:
                break
            offset += limit
        print(f"  ✔ TopGruposBR: {len(links)} links extraídos da base oficial!")
    except Exception as e:
        print(f"  Erro no TopGruposBR: {e}")
    return links


# ─── 2. WGRUPOS.COM (Categorias / Tags) ───────────────────────────────────────
def coletar_wgrupos() -> set:
    print("\n[2/7] 🕷️ Coletando WGrupos (Tags & Resolução de IDs)...")
    links = set()
    session = requests.Session()
    session.headers.update(HEADERS)
    
    tags = [
        "porno", "amizade", "xxx", "apostas-desportivas", "porno-gay",
        "pornografia", "onlyfans", "ofertas", "trans", "tecnologia",
        "sexo", "criptodivisas", "filmes", "hentai", "gamers", "packs",
        "noticias", "compra-venda", "religiao", "livros"
    ]
    all_ids = set()
    form_action = "/grupo/20261102"
    form_data = "20261102"

    for tag in tags:
        for p in range(1, 4):  # até 3 páginas por tag
            url = f"https://www.wgrupos.com/tag/telegram/{tag}/{p}" if p > 1 else f"https://www.wgrupos.com/tag/telegram/{tag}"
            try:
                r = session.get(url, timeout=8)
                if r.status_code == 200:
                    ids = re.findall(r"lnkgroup\(['\"]?(\d+)['\"]?\)", r.text)
                    all_ids.update(ids)
            except Exception:
                pass

    print(f"  WGrupos: {len(all_ids)} IDs de grupos encontrados. Resolvendo em paralelo...")
    target_post = "https://www.wgrupos.com/" + form_action.lstrip("/")

    def res_w(gid):
        try:
            r = session.post(target_post, data={"id": gid, "data": form_data}, timeout=7)
            if r.status_code == 200:
                return extrair_tme(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(res_w, gid): gid for gid in all_ids}
        for f in as_completed(futs):
            res = f.result()
            if res:
                links.update(res)

    print(f"  ✔ WGrupos: {len(links)} links reais do Telegram extraídos!")
    return links


# ─── 3. DIVULGRUPO.COM.BR (41 Categorias) ────────────────────────────────────
def coletar_divulgrupo() -> set:
    print("\n[3/7] 📂 Coletando Divulgrupo (41 Categorias)...")
    links = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    categorias = [
        "academia", "adulto", "amizade", "amor-e-romance", "animais", "apostas",
        "artesanato", "carros-e-motos", "cidades-e-regioes", "ciencia", "compra-e-venda",
        "concursos", "cortes-de-videos", "criptomoedas", "culinaria-e-receitas", "cursos",
        "desenhos-e-animes", "divulgacao", "educacao", "investimentos", "esportes",
        "evangelico", "eventos", "figurinhas-e-stickers", "filmes-e-series",
        "frases-e-mensagens", "ganhar-dinheiro", "imobiliaria", "lgbt", "links",
        "memes-e-humor", "moda-e-beleza", "motivacao-e-autoajuda", "musicas", "namoro",
        "ofertas-e-cupons", "saude-e-bem-estar", "seguidores-instagram", "sorteios-e-premiacoes",
        "tecnologia"
    ]

    urls_para_visitar = []
    for cat in categorias:
        urls_para_visitar.append(f"https://divulgrupo.com.br/categoria/{cat}")
        urls_para_visitar.append(f"https://divulgrupo.com.br/categoria/{cat}/2")
        urls_para_visitar.append(f"https://divulgrupo.com.br/categoria/{cat}/3")

    def fetch_divul(u):
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                return extrair_tme(r.text)
        except Exception:
            pass
        return set()

    with ThreadPoolExecutor(max_workers=20) as ex:
        futs = {ex.submit(fetch_divul, u): u for u in urls_para_visitar}
        for f in as_completed(futs):
            res = f.result()
            if res:
                links.update(res)

    print(f"  ✔ Divulgrupo: {len(links)} links reais do Telegram extraídos!")
    return links


# ─── 4. GRUPOSDOTELEGRAM.COM.BR (35 Categorias com Ajax & Decoder) ───────────
def coletar_gruposdotelegram() -> set:
    print("\n[4/7] 🔐 Coletando GruposDoTelegram (Ajax + Decodificador Base64 Reverso)...")
    links = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    # Coletar IDs de posts via Ajax
    urls_post = set()
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

    print(f"  GruposDoTelegram: {len(urls_post)} posts encontrados. Decodificando links em paralelo...")

    def decodificar_post(u):
        try:
            rp = session.get(u, timeout=8)
            if rp.status_code == 200:
                m = re.search(r'var tg_k\s*=\s*["\']([^"\']+)["\']', rp.text)
                if m:
                    raw_b64 = m.group(1)
                    dec = base64.b64decode(raw_b64).decode('utf-8', errors='ignore')[::-1]
                    c = clean_tg(dec)
                    if c:
                        return c
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=25) as ex:
        for res in ex.map(decodificar_post, urls_post):
            if res:
                links.add(res)

    print(f"  ✔ GruposDoTelegram: {len(links)} links reais do Telegram decodificados!")
    return links


# ─── 5. TELEGRUPOS.COM.BR (58 Categorias + Posts) ─────────────────────────────
def coletar_telegrupos() -> set:
    print("\n[5/7] 📑 Coletando TeleGrupos.com.br (Varredura de Posts)...")
    links = set()
    session = requests.Session()
    session.headers.update(HEADERS)

    # Varre as primeiras 25 páginas do site principal
    post_urls = set()
    for p in range(1, 26):
        u = f"https://telegrupos.com.br/page/{p}/" if p > 1 else "https://telegrupos.com.br/"
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    h = a['href'].strip().split('?')[0].rstrip('/')
                    txt = a.text.strip().lower()
                    if ("entrar" in txt or len(h) > 28) and h.startswith("https://telegrupos.com.br/"):
                        if not any(x in h for x in ["/page/", "/categoria/", "/tag/", "minha-conta", "termos", "politica"]):
                            post_urls.add(h)
        except Exception:
            pass

    print(f"  TeleGrupos: {len(post_urls)} posts identificados. Extraindo botões...")

    def fetch_tg_post(u):
        try:
            time.sleep(0.05)
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    if "entrar no grupo" in a.text.lower() and ("t.me" in a["href"] or "telegram" in a["href"]):
                        c = clean_tg(a["href"])
                        if c:
                            return c
                # Fallback: extrai t.me do html
                for t in extrair_tme(r.text):
                    return t
        except Exception:
            pass
        return None

    with ThreadPoolExecutor(max_workers=10) as ex:
        for res in ex.map(fetch_tg_post, post_urls):
            if res:
                links.add(res)

    print(f"  ✔ TeleGrupos: {len(links)} links reais do Telegram extraídos!")
    return links


# ─── 6. TELEGRUPOSBRASIL & METRICGRAM ─────────────────────────────────────────
def coletar_outros_diretorios() -> set:
    print("\n[6/7] 🌐 Coletando TeleGruposBrasil e MetricGram...")
    links = set()
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
        "https://telegruposbrasil.com/telegram-18",
        "https://metricgram.com/pt/tools/telegram-group-finder",
    ]

    for u in urls:
        try:
            r = session.get(u, timeout=8)
            if r.status_code == 200:
                links.update(extrair_tme(r.text))
        except Exception:
            pass

    print(f"  ✔ Outros portais: {len(links)} links extraídos!")
    return links


# ─── FLUXO PRINCIPAL DE EXECUÇÃO ──────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  🚀 SUPER GARIMPO DE TODOS OS SITES DE GRUPOS DO TELEGRAM BR")
    print("=" * 70)

    todos_coletados = set()

    # 1. TopGruposBR
    s1 = coletar_topgruposbr()
    todos_coletados.update(s1)

    # 2. WGrupos
    s2 = coletar_wgrupos()
    todos_coletados.update(s2)

    # 3. Divulgrupo
    s3 = coletar_divulgrupo()
    todos_coletados.update(s3)

    # 4. GruposDoTelegram
    s4 = coletar_gruposdotelegram()
    todos_coletados.update(s4)

    # 5. TeleGrupos.com.br
    s5 = coletar_telegrupos()
    todos_coletados.update(s5)

    # 6. Outros portais
    s6 = coletar_outros_diretorios()
    todos_coletados.update(s6)

    print("\n" + "=" * 70)
    print(f"  🎉 TOTAL DE LINKS TELEGRAM ÚNICOS COLETADOS: {len(todos_coletados)}")
    print("=" * 70)

    # 7. Passar tudo pelo filtro automático estrito
    print("\n[7/7] 🛡️ Passando pelo Filtro Estrito (Ativos + Só BR + Chat Aberto + Sem Duplicatas)...")
    texto_todos = "\n".join(sorted(todos_coletados))
    
    from moderador_engine import processar_e_adicionar_links
    res = processar_e_adicionar_links(texto_todos)

    print("\n" + "=" * 70)
    print("  📊 RELATÓRIO FINAL DA FILTRAGEM E ADIÇÃO AO SITE:")
    print("=" * 70)
    print(f"  📥 Total recebidos:        {res.get('total_recebidos')}")
    print(f"  🔬 Novos analisados HTTP:  {res.get('novos_analisados')}")
    print(f"  ❌ Expirados descartados:  {res.get('descartados_expirados')}")
    print(f"  ❌ Gringos descartados:    {res.get('descartados_gringos')}")
    print(f"  ❌ Canais/Sem chat:        {res.get('descartados_sem_chat')}")
    print(f"  🔁 Duplicados:             {res.get('duplicados')}")
    print(f"  ✅ Novos grupos aprovados: {res.get('novos_aprovados')}")
    print(f"  🏆 TOTAL NO SITE AGORA:    {res.get('total_atual_site')} grupos brasileiros ativos!")
    print("=" * 70)


if __name__ == "__main__":
    main()
