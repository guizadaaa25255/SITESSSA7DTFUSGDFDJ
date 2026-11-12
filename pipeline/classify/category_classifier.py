# -*- coding: utf-8 -*-
"""
pipeline/classify/category_classifier.py
Inferência automática da categoria de grupos com base no nome e na descrição.
"""

import re
from typing import Dict, List

CATEGORIAS_MAP: Dict[str, List[str]] = {
    "vagas_emprego": [
        "vaga", "vagas", "emprego", "empregos", "trabalho", "estagio", "estágio",
        "home office", "freelancer", "curriculo", "currículo", "trampo", "rh", "recrutamento"
    ],
    "concursos_estudo": [
        "concurso", "concursos", "enem", "vestibular", "oab", "estudo", "estudos",
        "concurseiro", "concurseiros", "direito", "faculdade", "universidade", "apostila",
        "apostilas", "pedagogia", "leitura", "livros", "redacao", "redação", "curso", "cursos"
    ],
    "tecnologia_programacao": [
        "programacao", "programação", "python", "javascript", "developer", "desenvolvedor",
        "dev", "linux", "software", "computacao", "computação", "ti", "frontend", "backend",
        "cloud", "cybersecurity", "ia", "inteligencia artificial", "dados", "ti brasil"
    ],
    "jogos_games": [
        "jogo", "jogos", "game", "games", "gamer", "gamers", "free fire", "roblox",
        "playstation", "ps4", "ps5", "xbox", "nintendo", "steam", "gta", "minecraft",
        "league of legends", "fortnite", "rpg"
    ],
    "cripto_financas": [
        "cripto", "criptomoedas", "bitcoin", "btc", "ethereum", "eth", "investimento",
        "investimentos", "financas", "finanças", "bolsa de valores", "acoes", "ações",
        "day trade", "trader", "renda fixa", "b3"
    ],
    "esportes_futebol": [
        "futebol", "brasileirao", "brasileirão", "flamengo", "corinthians", "palmeiras",
        "sao paulo fc", "santos", "vasco", "gremio", "grêmio", "cruzeiro", "esporte",
        "esportes", "cartola", "futsal", "campeonato"
    ],
    "filmes_series": [
        "filme", "filmes", "serie", "series", "séries", "cinema", "netflix", "streaming",
        "anime", "animes", "manga", "mangá", "desenhos", "cinema", "doramas"
    ],
    "vendas_comercio": [
        "compra e venda", "compras", "vendas", "classificados", "produtos", "shopee",
        "mercado livre", "afiliados", "dropshipping", "atacado", "varejo", "lojistas",
        "desapego", "brecho", "brechó", "importacao", "importação"
    ],
    "amizade_conversa": [
        "amizade", "amizades", "bate-papo", "bate papo", "conversa", "conversar",
        "resenha", "zoeira", "zueira", "papo", "crush", "namoro", "novos amigos",
        "galera", "desabafo", "desabafos"
    ],
    "musica_arte": [
        "musica", "música", "musicas", "músicas", "som", "banda", "cantor", "cantora",
        "sertanejo", "funk", "rock", "rap", "hip hop", "instrumentos", "partituras", "arte"
    ],
    "saude_fitness": [
        "academia", "fitness", "musculacao", "musculação", "treino", "dieta", "emagrecer",
        "emagrecimento", "nutricao", "nutrição", "saude", "saúde", "suplementos", "medicina"
    ],
    "automoveis": [
        "carro", "carros", "moto", "motos", "caminhao", "caminhões", "veiculos", "veículos",
        "mecanica", "mecânica", "rebaixados", "som automotivo", "tuning"
    ],
    "comunidades_locais": [
        "sao paulo", "rio de janeiro", "belo horizonte", "curitiba", "porto alegre",
        "salvador", "recife", "fortaleza", "goiania", "brasilia", "campinas", "regiao"
    ]
}


def classify_category(nome: str, descricao: str) -> str:
    """
    Classifica a categoria do grupo com base nas palavras-chave mais recorrentes.
    """
    texto = f"{nome} {descricao}".lower()

    melhor_categoria = "geral"
    maior_score = 0

    for cat, keywords in CATEGORIAS_MAP.items():
        score = 0
        for kw in keywords:
            # Ponderar se a palavra-chave estiver no título
            if re.search(r"\b" + re.escape(kw) + r"\b", nome, re.I):
                score += 3
            elif re.search(r"\b" + re.escape(kw) + r"\b", texto, re.I):
                score += 1

        if score > maior_score:
            maior_score = score
            melhor_categoria = cat

    return melhor_categoria
