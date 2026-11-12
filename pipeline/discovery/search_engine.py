# -*- coding: utf-8 -*-
"""
pipeline/discovery/search_engine.py
Gera consultas dorks direcionadas e garimpa links de grupos via DuckDuckGo.
Combina categorias × cidades brasileiras × termos de busca dinamicamente.
"""

import random
import time
import urllib.parse
from typing import List, Set, Tuple
import requests
from bs4 import BeautifulSoup
from pipeline.extract.extractor import canonicalize_url

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
}

TERMOS_CATEGORIA = [
    "concursos publicos", "enem vestibular", "oab direito", "estudantes medicina",
    "vagas ti brasil", "programacao python", "javascript dev", "empregos home office",
    "investimentos b3", "criptomoedas brasil", "renda extra pix",
    "futebol brasileirao", "torcedores flamengo", "torcida corinthians", "palmeiras grupo",
    "filmes series dublado", "animes brasil", "jogos games brasil", "free fire brasil",
    "amizades bate papo", "conversa resenha", "compras vendas brasil", "desapego sp",
    "academia treino musculacao", "nutricao saude", "carros motos brasil",
    "musica sertanejo", "livros pdf estudos"
]

CIDADES_ESTADOS = [
    "brasil", "sao paulo", "rio de janeiro", "belo horizonte", "curitiba",
    "porto alegre", "salvador", "recife", "fortaleza", "brasilia", "goiania",
    "campinas", "manaus", "belem", "florianopolis", "vitoria", "cuiaba",
    "campo grande", "natal", "joao pessoa", "maceio", "sp", "rj", "mg", "ba", "pr", "rs"
]

TEMPLATES = [
    'site:t.me "grupo" {cat} {loc}',
    'site:t.me/+ "{cat}" {loc}',
    'site:t.me/joinchat "{cat}"',
    'site:t.me "chat" "{cat}" brasil',
    '"t.me/+" "{cat}" {loc}',
    '"t.me/joinchat" "{cat}" brasil',
    'site:t.me "grupo" {cat}',
]


def gerar_dorks_dinamicos(qtd: int = 20) -> List[str]:
    """Gera combinações aleatórias de dorks a partir das categorias e cidades."""
    dorks = []
    usados = set()
    for _ in range(qtd * 3):
        tpl = random.choice(TEMPLATES)
        cat = random.choice(TERMOS_CATEGORIA)
        loc = random.choice(CIDADES_ESTADOS)
        q = tpl.format(cat=cat, loc=loc).strip()
        if q not in usados:
            usados.add(q)
            dorks.append(q)
            if len(dorks) >= qtd:
                break
    return dorks


def search_duckduckgo(query: str, session: requests.Session) -> Set[str]:
    """Executa uma busca no DuckDuckGo HTML e extrai links do t.me."""
    links: Set[str] = set()
    url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote_plus(query)}"
    try:
        r = session.get(url, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "uddg=" in href:
                    parsed = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
                    target = parsed.get("uddg", [""])[0]
                else:
                    target = href

                if "t.me" in target:
                    c = canonicalize_url(target)
                    if c:
                        links.add(c[0])
    except Exception as e:
        pass

    return links


def run_search_dorks(max_queries: int = 15, custom_queries: List[str] = None) -> List[Tuple[str, str]]:
    """Executa consultas de dorks limitando a taxa para evitar bloqueios."""
    queries = custom_queries if custom_queries else gerar_dorks_dinamicos(max_queries)
    print(f"[Discovery] 🔍 Executando dorks de busca no DuckDuckGo ({len(queries)} consultas)...")
    session = requests.Session()
    session.headers.update(HEADERS)

    encontrados: Set[str] = set()
    for q in queries:
        novos = search_duckduckgo(q, session)
        encontrados.update(novos)
        time.sleep(1.2)

    print(f"[Discovery] ✔ Busca DuckDuckGo: {len(encontrados)} links únicos encontrados.")
    return [(l, "duckduckgo_dork") for l in encontrados]
