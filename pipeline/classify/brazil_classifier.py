# -*- coding: utf-8 -*-
"""
pipeline/classify/brazil_classifier.py
Classificador linguístico e geográfico de pertinência ao Brasil (pt-BR).
Calcula score_brasil (0.0 a 1.0) e filtra apenas grupos com score >= 0.80.
Descarta scripts estrangeiros, termos de outros idiomas e marcadores de Portugal (pt-PT).
"""

import re
import unicodedata
from typing import Optional, Tuple
import langdetect
from langdetect import DetectorFactory

DetectorFactory.seed = 42

# Scripts estrangeiros não-latinos (descarte imediato)
RE_FOREIGN_SCRIPTS = re.compile(
    r"[\u0400-\u04FF"          # Cirílico
    r"\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF" # Árabe, Farsi, Urdu
    r"\u4E00-\u9FFF\u3400-\u4DBF"               # Chinês
    r"\uAC00-\uD7AF\u1100-\u11FF"               # Coreano
    r"\u3040-\u309F\u30A0-\u30FF"               # Japonês
    r"\u0E00-\u0E7F"                           # Tailandês
    r"\u0900-\u0DFF"                           # Índico
    r"\u0590-\u05FF"                           # Hebraico
    r"\u1000-\u109F"                           # Myanmar
    r"\u1EA0-\u1EF9"                           # Vietnamita estendido
    r"\u011E\u011F\u0130\u0131\u015E\u015F\u0259\u018F" # Turco/Azeri
    r"]"
)

# Bandeiras estrangeiras
RE_FLAGS = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")

# Marcadores inequívocos de Portugal / pt-PT (descarte imediato)
RE_PT_PORTUGAL = re.compile(
    r"\b("
    r"autocarro|autocarros|telemóvel|telemovel|telemóveis|telemoveis|"
    r"ecrã|ecrãs|ecra|ecras|pequeno-almoço|pequeno almoço|"
    r"comboio|comboios|propina|propinas|chávena|chavenas|"
    r"fato de treino|fatos de treino|tuga|tugas|gajo|gaja|gajos|gajas|"
    r"portugal|lisboa|coimbra|braga|faro|setúbal|aveiro|algarve|"
    r"porto(?!\s+(?:alegre|velho|seguro|nacional|real|murtinho|esperança|esperanca|rico|ferreira|feliz|belo))|"
    r"angola|luanda|moçambique|mocambique|mozambique|maputo|cabo verde|guiné-bissau|guine-bissau|são tomé|sao tome"
    r")\b",
    re.IGNORECASE,
)

# Vocábulos gringos característicos (descarte implacável)
RE_FOREIGN_WORDS = re.compile(
    r"\b("
    # Espanhol
    r"bienvenidos|bienvenido|bienvenida|chicas|chicos|morras|morros|hispano|hispanos|"
    r"argentina|mexico|colombia|venezuela|chile|peru|uruguay|paraguay|bolivia|ecuador|"
    r"peliculas|películas|enlace|unete|únete|solicitar|unirse|hablar|amigos virtuales|"
    r"todo el mundo|todos los|conocer gente|hacer amigos|para compartir|comunidad hispana|"
    r"comunidad latina|latinos|latinas|sorteos|prueba de|referidos|cerrado|compraventa|"
    r"apuestas|pronosticos|futbol en vivo|series y peliculas|"
    # Inglês
    r"welcome to|official group|official chat|discussion group|earn money|airdrop|crypto pump|"
    r"instant withdrawal|free signals|registered members|worldwide community|giveaway|"
    r"join now|telegram group|trading signals|investing|forex trading|crypto gems|vip signals|"
    r"wallpapers|memes daily|gaming community|onlyfans leak|nude leak|premium leaks|"
    r"tiktok viral|dating community|meet singles|buy sell|dropshipping global|"
    # Russo / Eslavo
    r"sliv|slivy|skladchik|privat|znakomstva|rossiya|moscow|chatik|novosti|skidki|"
    # Indonésio / Asiático
    r"slot gacor|maxwin|pragmatic|zeus|lpm|jual|beli|cari teman|lowongan|kerja|indo viral|"
    # Turco / Ásia Central
    r"sohbet|tanishuv|guruh|dostlar|tashkent|uzbekistan|"
    # Alemão / Francês / Italiano
    r"willkommen|plauderecke|plaudern|herzlich|regeln|nerds seid|deutschland|schweiz|"
    r"bienvenue|groupe officiel|rencontre|benvenuti|gruppo ufficiale"
    r")\b",
    re.IGNORECASE,
)

# Marcadores de brasilidade (pt-BR, estados, capitais, cidades do interior e termos culturais)
# Testado contra texto sem acento para cobrir com/sem acentuação perfeitamente
RE_BR_STRONG = re.compile(
    r"\b("
    r"brasil|brazil|brasileiro|brasileira|brasileiros|brasileiras|"
    r"sao paulo|rio de janeiro|minas gerais|bahia|parana|rio grande do sul|"
    r"santa catarina|ceara|pernambuco|goias|distrito federal|brasilia|"
    r"espirito santo|mato grosso|maranhao|piaui|rio grande do norte|paraiba|alagoas|sergipe|"
    r"amazonas|rondônia|rondonia|tocantins|acre|amapa|roraima|"
    r"curitiba|porto alegre|porto velho|porto seguro|belo horizonte|salvador|fortaleza|recife|manaus|belem|goiania|"
    r"vitoria|vila velha|maceio|natal|teresine|teresina|joao pessoa|aracaju|sao luis|cuiaba|campo grande|florianopolis|macapa|boa vista|rio branco|palmas|"
    r"campinas|guarulhos|sao bernardo|santo andre|osasco|sorocaba|ribeirao preto|sao jose dos campos|santos|jundiai|piracicaba|bauru|franca|taubate|barueri|suzano|taboao|sumare|indaiatuba|cotia|sao carlos|marilia|araraquara|jacarei|hortolandia|presidente prudente|americana|itu|atibaia|itapetininga|botucatu|caraguatatuba|guaruja|praia grande|sao vicente|mogi|"
    r"uberlandia|contagem|juiz de fora|betim|montes claros|uberaba|governador valadares|ipatinga|sete lagoas|divinopolis|pocos de caldas|patos de minas|pouso alegre|varginha|araguari|itabira|passos|lavras|itajuba|paracatu|caratinga|patrocinio|manhuacu|unai|curvelo|alfenas|ouro preto|janauba|"
    r"sao goncalo|duque de caxias|nova iguacu|niteroi|belford roxo|campos dos goytacazes|petropolis|volta redonda|macae|mage|itaborai|cabo frio|angra dos reis|nova friburgo|barra mansa|teresopolis|marica|rio das ostras|buzios|paraty|"
    r"feira de santana|vitoria da conquista|camacari|juazeiro|itabuna|lauro de freitas|ilheus|jequie|teixeira de freitas|barreiras|alagoinhas|paulo afonso|eunapolis|jacobina|irece|brumado|"
    r"jaboatao|olinda|caruaru|petrolina|paulista|camaragibe|garanhuns|igarassu|serra talhada|araripina|gravata|carpina|arcoverde|surubim|salgueiro|"
    r"caucaia|maracanau|sobral|crato|itapipoca|maranguape|iguatu|quixada|pacatuba|aquiraz|quixeramobim|caninde|russas|crateus|tiangua|aracati|cascavel|pacajus|ico|camocim|morada nova|barbalha|limoeiro|taua|trairi|acarau|beberibe|eusebio|brejo santo|baturite|paracuru|"
    r"londrina|maringa|ponta grossa|sao jose dos pinhais|foz do iguacu|colombo|guarapuava|paranagua|araucaria|toledo|apucarana|pinhais|campo largo|arapongas|umuarama|cambe|francisco beltrao|paranavai|pato branco|cianorte|"
    r"caxias do sul|canoas|pelotas|santa maria|gravatai|viamao|novo hamburgo|sao leopoldo|alvorada|passo fundo|uruguaiana|santa cruz do sul|bage|bento goncalves|erechim|guaiba|ijui|lajeado|farroupilha|"
    r"joinville|blumenau|chapeco|itajai|criciuma|jaragua do sul|palhoca|lages|balneario camboriu|brusque|tubarao|cacador|navegantes|concordia|itapema|"
    r"aparecida de goiania|anapolis|rio verde|luziania|valparaiso|trindade|formosa|senador canedo|itumbiara|catalao|jatai|caldas novas|taguatinga|ceilandia|samambaia|aguas claras|gama|guara|sobradinho|"
    r"imperatriz|timon|codo|acailandia|balsas|parnaiba|picos|mossoro|parnamirim|caico|campina grande|santa rita|patos|cabedelo|cajazeiras|arapiraca|penedo|lagarto|itabaiana|"
    r"ananindeua|santarem|maraba|parauapebas|castanhal|abaetetuba|altamira|tucurui|paragominas|itaituba|parintins|itacoatiara|manacapuru|ji-parana|ariquemes|vilhena|cacoal|araguaina|gurupi|cruzeiro do sul|dourados|tres lagoas|corumba|ponta pora|rondonopolis|sinop|tangara da serra|caceres|sorriso|lucas do rio verde|primavera do leste|barra do garcas|cachoeiro de itapemirim|linhares|colatina|guarapari|"
    r"brasileirao|corinthians|flamengo|palmeiras|santos fc|sao paulo fc|cruzeiro|gremio|vasco|botafogo|fluminense|atletico mg|athletico|ceara sc|fortaleza ec|bahia ec|sport recife|"
    r"enem|vestibular|concurso publico|concursos publicos|oab|concurseiros|fgv|cebraspe|vunesp|prefeitura|detran|inss|"
    r"pix|chave pix|nubank|renda extra|caixa tem|shopee|mercado livre|achadinhos|"
    r"resenha|zoeira|zueira|bate-papo|bate papo|conversa fiada|galera|pessoal|mocada|parceiros|parceiras|"
    r"amizade|amizades|namoro|paquera|solteiros|solteiras|relatos|desabafo|desabafos|vagas|empregos|cursos|apostilas|compra e venda|compras e vendas|vendas e compras| desapego|classificados|ofertas|promocoes|"
    r"zona leste|zona sul|zona norte|zona oeste|baixada|regiao|interior|"
    r"sp|rj|mg|ba|pr|rs|sc|pe|ce|go|df|es|am|pa|mt|ms|rn|pb|al|se|pi|ma|to|ro|ac|ap|rr|"
    r"ddd\s*(?:1[1-9]|[2-9][0-9])"
    r")\b",
    re.IGNORECASE,
)

# Estrutura gramatical e vocabulário inequívoco do português
RE_PT_GRAMMAR = re.compile(
    r"\b("
    r"voce|voces|nao|entao|tambem|aqui|muito|alguem|ninguem|nosso|nossa|nossos|nossas|"
    r"pra|pro|comigo|contigo|amigos|amigas|duvidas|regras|proibido|seja|sejam|bem-vindo|bem-vindos|bem vindo|bem vindos|"
    r"grupo|grupos|conversa|conversar|bate|papo|falar|entrar|entre|participe|participar|respeito|todos|todas|melhor|melhores|"
    r"do|da|dos|das|no|na|nos|nas|para|com|sem|pelo|pela|pelos|pelas|de|em|um|uma|uns|umas|ao|aos"
    r")\b",
    re.IGNORECASE,
)

RE_PT_DIACRITICS = re.compile(r"[áéíóúâêôãõçÁÉÍÓÚÂÊÔÃÕÇ]")


def normalize_unicode_text(text: str) -> str:
    """Normaliza símbolos Unicode estilizados (ex: math bold/italic) mantendo caracteres acentuados compostos (NFKC)."""
    if not text:
        return ""
    nfkc = unicodedata.normalize("NFKC", text)
    # Substituir homógrafos comuns (como 'օ' armênio usado em títulos estilizados)
    nfkc = nfkc.replace("\u0585", "o").replace("\u0430", "a").replace("\u0435", "e").replace("\u043e", "o")
    t = re.sub(r"https?://\S+", " ", nfkc)
    t = re.sub(r"@[a-zA-Z0-9_]+", " ", t)
    t = re.sub(r"[#*_~`\[\]()<>{}|\\]", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def strip_accents(text: str) -> str:
    """Remove acentos para comparação insensível a acentuação."""
    if not text:
        return ""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def check_brazil_confidence(nome: str, descricao: str, url: str = "") -> Tuple[bool, float, str]:
    """
    Avalia a pertinência brasileira de um grupo.
    Retorna (is_brazil, score_brasil, idioma_detectado).
    Critério obrigatório: is_brazil é True somente quando score >= 0.80.
    """
    texto_combinado = f"{nome} {descricao}".strip()
    texto_limpo = normalize_unicode_text(texto_combinado)
    texto_sem_acento = strip_accents(texto_limpo)

    # 1. Scripts estrangeiros não-latinos
    if RE_FOREIGN_SCRIPTS.search(texto_limpo):
        return False, 0.0, "outro_script"

    # 2. Bandeiras
    bandeiras = RE_FLAGS.findall(texto_combinado)
    tem_bandeira_br = "🇧🇷" in bandeiras
    tem_outra_bandeira = any(b != "🇧🇷" for b in bandeiras)
    if tem_outra_bandeira and not tem_bandeira_br:
        return False, 0.05, "bandeira_estrangeira"

    # 3. Marcadores inequívocos de Portugal (pt-PT)
    if RE_PT_PORTUGAL.search(texto_limpo) or RE_PT_PORTUGAL.search(texto_sem_acento):
        if not tem_bandeira_br and not re.search(r"\b(brasil|brasileiro|brasileira|br)\b", texto_sem_acento, re.I):
            return False, 0.10, "pt-PT"

    # 4. Detecção de idioma com langdetect (usando texto NFKC com acentos preservados)
    idioma = "indeterminado"
    palavras = [w for w in re.split(r"\W+", texto_limpo) if len(w) > 1]
    if len(palavras) >= 3:
        try:
            idioma = langdetect.detect(texto_limpo)
        except Exception:
            idioma = "indeterminado"

    # 5. Termos gringos (Descarte imediato de qualquer menção estrangeira explícita)
    matches_gringos = RE_FOREIGN_WORDS.findall(texto_limpo) or RE_FOREIGN_WORDS.findall(texto_sem_acento)
    if matches_gringos:
        if not tem_bandeira_br and not re.search(r"\b(brasil|brasileiro|brasileira|br)\b", texto_sem_acento, re.I):
            return False, 0.0, f"gringo_{matches_gringos[0]}"
        if idioma not in ("pt", "indeterminado"):
            return False, 0.05, idioma

    # 6. Cálculo do score
    score = 0.0
    matches_br = RE_BR_STRONG.findall(texto_sem_acento)
    matches_gram = RE_PT_GRAMMAR.findall(texto_sem_acento)
    tem_diacritico_pt = bool(RE_PT_DIACRITICS.search(texto_combinado) or RE_PT_DIACRITICS.search(texto_limpo))

    if idioma == "pt" or (len(palavras) < 8 and (matches_br or len(matches_gram) >= 2 or tem_diacritico_pt)):
        score += 0.50
    elif idioma == "indeterminado":
        score += 0.30
    elif idioma in ("es", "it", "gl", "ca", "ro", "fr", "id", "hu", "af", "nl") and (matches_br or matches_gram):
        # Textos curtos em português frequentemente são confundidos pelo langdetect
        score += 0.35
    else:
        score -= 0.25

    # Marcadores brasileiros (testados no texto sem acento para cobrir 100% das grafias)
    if tem_bandeira_br:
        score += 0.40
    if matches_br:
        score += min(0.55, len(matches_br) * 0.25)

    # Gramática portuguesa e acentuação
    if matches_gram:
        score += min(0.30, len(matches_gram) * 0.08)

    if tem_diacritico_pt:
        score += 0.20

    # Menção a BR na URL ou nome
    if re.search(r"(_br|brasil|brazil|br$)\b", url, re.I) or re.search(r"\bbr\b", texto_sem_acento, re.I):
        score += 0.20

    if matches_gringos:
        score -= len(matches_gringos) * 0.30

    score = max(0.0, min(1.0, score))
    score = round(score, 2)

    is_brazil = score >= 0.80
    lang_result = "pt" if (idioma == "pt" or score >= 0.80) else idioma

    return is_brazil, score, lang_result
