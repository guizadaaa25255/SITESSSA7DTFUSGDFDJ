# -*- coding: utf-8 -*-
"""
pipeline/discovery/local_ingest.py
Módulo para carregar e importar dados de coletas anteriores e arquivos locais do projeto,
alimentando a fila de processamento e aproveitando dados já raspados em cache.
"""

import json
import os
from typing import Any, Dict, List, Set, Tuple
from pipeline.extract.extractor import canonicalize_url


def load_local_links(base_dir: str) -> List[Tuple[str, str]]:
    """Carrega links de arquivos texto locais (links.txt, links_br.txt, etc.)."""
    encontrados: Set[str] = set()
    resultado: List[Tuple[str, str]] = []

    arquivos = [
        (os.path.join(base_dir, "dados", "links.txt"), "arquivo_links_txt"),
        (os.path.join(base_dir, "dados", "links_br.txt"), "arquivo_links_br_txt"),
        (os.path.join(base_dir, "dados", "links_descobertos_turbo.txt"), "arquivo_links_turbo"),
        (os.path.join(base_dir, "dados", "links_usuario_prompt.txt"), "arquivo_links_usuario"),
    ]

    for fpath, fonte in arquivos:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            c = canonicalize_url(line)
                            if c:
                                canon, _, dedup_key = c
                                if dedup_key not in encontrados:
                                    encontrados.add(dedup_key)
                                    resultado.append((canon, fonte))
            except Exception as e:
                print(f"[Aviso] Falha ao ler {fpath}: {e}")

    return resultado


def load_supabase_bruto(base_dir: str) -> List[Dict[str, Any]]:
    """Carrega dados brutos do Supabase TopGruposBR se disponíveis."""
    fpath = os.path.join(base_dir, "dados", "supabase_bruto.json")
    if not os.path.exists(fpath):
        return []

    try:
        with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception as e:
        print(f"[Aviso] Falha ao carregar supabase_bruto.json: {e}")
    return []


def load_coleta_tudo_cache(base_dir: str) -> Dict[str, Dict[str, Any]]:
    """Carrega o cache de coletas e previews HTML já realizados em coleta_tudo.jsonl."""
    fpath = os.path.join(base_dir, "resultados", "coleta_tudo.jsonl")
    cache: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(fpath):
        return cache

    try:
        with open(fpath, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        d = json.loads(line)
                        u = d.get("url")
                        if u:
                            c = canonicalize_url(u)
                            if c:
                                _, _, dedup_key = c
                                cache[dedup_key] = d
                    except Exception:
                        pass
    except Exception as e:
        print(f"[Aviso] Falha ao ler coleta_tudo.jsonl: {e}")

    return cache
