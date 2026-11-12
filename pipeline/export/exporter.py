# -*- coding: utf-8 -*-
"""
pipeline/export/exporter.py
Exporta a base de dados de grupos validados para CSV e JSON conforme as especificações,
e gera relatório estruturado de auditoria.
"""

import csv
import json
import os
from typing import Any, Dict, List


def export_to_csv(data: List[Dict[str, Any]], filepath: str):
    """Exporta para CSV com cabeçalho padrão e codificação UTF-8 com BOM para Excel."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    fields = [
        "link_convite", "username", "nome", "descricao", "membros",
        "tipo", "idioma_detectado", "score_brasil", "categoria",
        "fonte_de_origem", "data_coleta"
    ]

    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def export_to_json(data: List[Dict[str, Any]], filepath: str):
    """Exporta para JSON formatado com indentação e caracteres UTF-8 intactos."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def generate_summary_stats(data: List[Dict[str, Any]], stats_db: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula estatísticas de distribuição de membros, categorias e fontes."""
    total = len(data)
    faixas_membros = {
        "1 a 50": 0,
        "51 a 200": 0,
        "201 a 1000": 0,
        "1001 a 5000": 0,
        "5001+": 0
    }

    fontes = {}
    categorias = {}

    for item in data:
        m = item.get("membros") or 0
        if m <= 50:
            faixas_membros["1 a 50"] += 1
        elif m <= 200:
            faixas_membros["51 a 200"] += 1
        elif m <= 1000:
            faixas_membros["201 a 1000"] += 1
        elif m <= 5000:
            faixas_membros["1001 a 5000"] += 1
        else:
            faixas_membros["5001+"] += 1

        fonte = item.get("fonte_de_origem") or "desconhecido"
        fontes[fonte] = fontes.get(fonte, 0) + 1

        cat = item.get("categoria") or "geral"
        categorias[cat] = categorias.get(cat, 0) + 1

    return {
        "total_registros_validos": total,
        "faixas_membros": faixas_membros,
        "distribuicao_categorias": categorias,
        "distribuicao_fontes": fontes,
        "descartes_por_motivo": stats_db.get("descartes_por_motivo", {}),
        "total_descartados": stats_db.get("total_descartados", 0),
        "total_analisados": stats_db.get("total_descobertos", 0),
    }
