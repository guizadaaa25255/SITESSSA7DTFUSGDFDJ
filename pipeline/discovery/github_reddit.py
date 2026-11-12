# -*- coding: utf-8 -*-
"""
pipeline/discovery/github_reddit.py
Descobre links públicos de grupos em repositórios abertos do GitHub e no Reddit.
"""

import re
from typing import List, Set, Tuple
import requests

from pipeline.extract.extractor import extract_telegram_links

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

GITHUB_RAW_LISTS = [
    ("https://raw.githubusercontent.com/devfelipemonteiro/tecnologia-telegram-brasil/master/README.md", "github_tecnologia_br"),
    ("https://raw.githubusercontent.com/TI-Brasil/lista-telegram-brasil/master/README.md", "github_ti_brasil"),
    ("https://raw.githubusercontent.com/ti-no-canada/ti-no-canada.github.io/master/README.md", "github_ti_brasil_comunidade"),
]

REDDIT_SUBREDDITS = ["brasil", "brdev", "conversas", "concursospublicos"]


def fetch_github_curated_lists() -> List[Tuple[str, str]]:
    """Baixa listas públicas e curadas de grupos brasileiros hospedadas no GitHub."""
    print("[Discovery] 🐙 Coletando repositórios e listas do GitHub...")
    encontrados: Set[Tuple[str, str]] = set()

    session = requests.Session()
    session.headers.update(HEADERS)

    # 1. Listas estáticas conhecidas
    for url, fonte in GITHUB_RAW_LISTS:
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 200:
                links = extract_telegram_links(r.text)
                for l in links:
                    encontrados.add((l, fonte))
        except Exception as e:
            print(f"[Discovery] Falha ao ler {url}: {e}")

    # 2. Busca na API pública do GitHub
    try:
        api_url = "https://api.github.com/search/repositories?q=grupos+telegram+brasil&per_page=10"
        r = session.get(api_url, timeout=10)
        if r.status_code == 200:
            items = r.json().get("items", [])
            for it in items:
                repo_name = it.get("full_name")
                default_branch = it.get("default_branch", "master")
                readme_url = f"https://raw.githubusercontent.com/{repo_name}/{default_branch}/README.md"
                try:
                    r_read = session.get(readme_url, timeout=6)
                    if r_read.status_code == 200:
                        links = extract_telegram_links(r_read.text)
                        for l in links:
                            encontrados.add((l, f"github_{repo_name}"))
                except Exception:
                    pass
    except Exception as e:
        print(f"[Discovery] Aviso API GitHub: {e}")

    print(f"[Discovery] ✔ GitHub: {len(encontrados)} links de grupos coletados.")
    return list(encontrados)


def fetch_reddit_public_mentions() -> List[Tuple[str, str]]:
    """Consulta endpoints públicos de busca do Reddit em subreddits brasileiros."""
    print("[Discovery] 🔴 Coletando menções públicas no Reddit...")
    encontrados: Set[Tuple[str, str]] = set()
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})

    for sub in REDDIT_SUBREDDITS:
        try:
            url = f"https://www.reddit.com/r/{sub}/search.json?q=t.me&restrict_sr=1&limit=25"
            r = session.get(url, timeout=8)
            if r.status_code == 200:
                data = r.json()
                children = data.get("data", {}).get("children", [])
                for item in children:
                    selftext = item.get("data", {}).get("selftext", "")
                    title = item.get("data", {}).get("title", "")
                    content = f"{title}\n{selftext}"
                    links = extract_telegram_links(content)
                    for l in links:
                        encontrados.add((l, f"reddit_r_{sub}"))
        except Exception:
            pass

    print(f"[Discovery] ✔ Reddit: {len(encontrados)} links coletados.")
    return list(encontrados)
