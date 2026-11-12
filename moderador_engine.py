# -*- coding: utf-8 -*-
"""
moderador_engine.py
===================
Módulo de moderação e filtragem automática com as 3 regras:
1. Sem expirados / inativos (membros >= 5, link válido)
2. 100% Brasileiro (sem gringos, sem scripts estrangeiros, sem bandeiras estrangeiras)
3. Com chat aberto (sem canais, sem grupos trancados/sem chat, sem murais de spam)
Deduplicação total (zero repetidos).
"""

import html
import json
import os
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COLETA_JSONL = os.path.join(BASE_DIR, "resultados", "coleta_tudo.jsonl")
DESCARTADOS_TXT = os.path.join(BASE_DIR, "dados", "descartados_sem_chat.txt")
EXPIRADOS_TXT = os.path.join(BASE_DIR, "dados", "expirados.txt")
BR_JSON = os.path.join(BASE_DIR, "resultados", "br_validados.json")

RE_LINK = re.compile(r"(?:https?://)?(?:www\.)?(?:t|telegram)\.me/[a-zA-Z0-9_+\-]+(?:/[0-9]+)?|@[a-zA-Z0-9_]{3,}", re.I)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
}

RE_TITLE = re.compile(r'<div class="tgme_page_title"[^>]*>\s*<span[^>]*>(.*?)</span>', re.S)
RE_EXTRA = re.compile(r'<div class="tgme_page_extra"[^>]*>(.*?)</div>', re.S)
RE_DESC = re.compile(r'<div class="tgme_page_description"[^>]*>(.*?)</div>', re.S)
RE_IMG = re.compile(r'<img class="tgme_page_photo_image"[^>]*src="([^"]+)"', re.S)

def clean_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", " ", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()

def parse_membros(extra: str) -> int:
    if not extra:
        return 0
    part = extra.split(",")[0]
    nums = re.sub(r"[^0-9]", "", part)
    return int(nums) if nums else 0

def canon_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip().replace("http://", "https://").replace("https://telegram.me/", "https://t.me/").replace("http://telegram.me/", "https://t.me/")
    if u.startswith("@"):
        u = "https://t.me/" + u[1:]
    if not u.startswith("https://"):
        if u.startswith("t.me/"):
            u = "https://" + u
        elif u.startswith("telegram.me/"):
            u = "https://t.me/" + u[12:]
        else:
            u = "https://t.me/" + u
    u = u.split("?")[0].rstrip("/")
    m2 = re.match(r"^(https://t\.me/[a-zA-Z0-9_]+)/(?:\d+)$", u)
    if m2 and not u.startswith("https://t.me/c/") and not u.startswith("https://t.me/joinchat/"):
        u = m2.group(1)
    m = re.match(r"^https://t\.me/joinchat/([a-zA-Z0-9_-]+)$", u, re.I)
    if m:
        return f"https://t.me/+{m.group(1)}"
    return u

def url_key(u: str) -> str:
    c = canon_url(u)
    if "/+" in c or "/joinchat/" in c.lower():
        return c
    return c.lower()

SMALL_CAPS_MAP = str.maketrans({
    "ᴀ": "a", "ʙ": "b", "ᴄ": "c", "ᴅ": "d", "ᴇ": "e", "ꜰ": "f", "ɢ": "g", "ʜ": "h",
    "ɪ": "i", "ᴊ": "j", "ᴋ": "k", "ʟ": "l", "ᴍ": "m", "ɴ": "n", "ᴏ": "o", "ᴘ": "p",
    "ǫ": "q", "ʀ": "r", "ꜱ": "s", "ᴛ": "t", "ᴜ": "u", "ᴠ": "v", "ᴡ": "w", "ʏ": "y", "ᴢ": "z",
    "α": "a", "є": "e", "і": "i", "о": "o", "υ": "u", "η": "n", "м": "m", "σ": "o", "Δ": "a", "Π": "n"
})

def norm_text(s: str) -> str:
    if not s:
        return ""
    s = s.translate(SMALL_CAPS_MAP)
    n = unicodedata.normalize("NFKD", s)
    return "".join([c for c in n if not unicodedata.combining(c)]).lower()

def strip_urls(s: str) -> str:
    s = re.sub(r"https?://\S+", " ", s, flags=re.I)
    s = re.sub(r"\b\S+\.(com|org|net|io|ai|cloud|gg|me|br|co|cu|es|pt|ru|ly|ph|it|ee|app|in)\b\S*", " ", s, flags=re.I)
    s = re.sub(r"@[a-zA-Z0-9_]+", " ", s)
    s = re.sub(r"/(rules|report|start|help|notes|link)\b", " ", s, flags=re.I)
    return s

RE_SCRIPT_GRINGO = re.compile(
    r"[\u0370-\u03FF\u0400-\u052F\u0530-\u058F\u0590-\u05FF\u0600-\u077F\u0900-\u109F\u10A0-\u10FF\u1200-\u137F"
    r"\u1780-\u17FF\u0E00-\u0E7F\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF"
    r"\u0102\u0103\u0110\u0111\u01A0\u01A1\u01AF\u01B0\u1EA0-\u1EF9\u0141\u0142\u015A\u015B\u0106\u0107\u0179\u017A\u017B\u017C"
    r"\u011E\u011F\u0130\u0131\u015E\u015F\u02BB\u02BC]"
)

RE_BANDEIRA = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")
def tem_bandeira_gringa(s: str) -> bool:
    for m in RE_BANDEIRA.findall(s):
        if m != "🇧🇷":
            return True
    return False

RE_SEM_CHAT = re.compile(
    r"("
    r"\bsomente administradores\b|\bapenas administradores\b|\bgrupo fechado\b|\bchat fechado\b|\bchat trancado\b|\bgrupo desativado\b|"
    r"\bapenas avisos\b|\bsomente leitura\b|\bnao e permitido enviar mensagens\b|\bsomente com adm\b|\bconstituido apenas de bots\b|"
    r"\bsecretinhos\b|\bstars\b|\bestrelas\b|\bpago por mensagem\b|\btaxa por mensagem\b|\bpague para\b|\bcobrança\b|\bcobrar\b|"
    r"achadinhos|promoecupons|\bcupons\b|descontador|radar de ofertas|central de ofertas|foca promocoes|produtosshop|"
    r"of3rtassemanais|ofertasepromocoes|ofertavemki|promochina|promomercadolivre|videos_e_ofertas|babypromoclick|promodemamae|"
    r"só vazados|so vazados|prévias|previas|packsfree|"
    r"robos da blaze|\bsinais free\b|\bsala de sinal\b|\bsinais famosos\b|melhores salas de sinais|tigre da aposta|horario fortune|7733 zemvix|ad-7x7|games7x7|7topwin|grupodeslots|grupo de slots|"
    r"venha ser cliente iptv|infinity iptv|telux streamings|will solutions|multicontas|spotify prmium|\bacervo\b|biblioteca virtual"
    r")",
    re.I
)

RE_GRINGO = re.compile(
    r"\b("
    r"el|los|las|del|una|unos|unas|pero|quien|cuando|donde|nuestra|nuestro|nosotros|vosotros|espanol|"
    r"bienvenidos|bienvenido|reglas|prohibido|amistad|amigos virtuales|hablar|chicas|chicos|novia|pareja|"
    r"comunidad|oposicion|cuerpo|provincia|cuba|cubano|habana|santiago de cuba|matanzas|tienda|"
    r"compartir|peliculas|subtitulos|difundir|contactos|agregarse|pertenece|siguelas|solamente|"
    r"enfocado|eres|inversor|preguntas|martes|jueves|quedadas|cuestiones|incumpla|sociedad|"
    r"compraventa|compra y venta|unete|enlace|canal de|grupo de amistad|conocer gente|sin faltas|"
    r"respeto a todos|buenos dias|buenas tardes|buenas noches|hola a todos|mujeres|hombres|"
    r"argentina|mexico|colombia|venezuela|peru|chile|ecuador|bolivia|uruguay|paraguay|espana|madrid|barcelona|alicante|"
    r"the|and|for|with|this|that|from|have|your|you|are|only|allowed|welcome|join|chatting|friends|"
    r"building|faster|scalable|based|powered|website|monitoring|official chat|official group|community|global|worldwide|"
    r"english|usa|uk|canada|india|pakistan|bangladesh|nigeria|philippines|indonesia|malaysia|vietnam|ethiopia|australia|sydney|melbourne|"
    r"cricket|ipl|teen patti|rummy|casino night|instant withdrawal|trusted platform|register now|discussion group|informational|"
    r"earn money|earning|looters|fraud|karor|kache|jimmedar|nhi|honge|bnde|koi|ambala|bgmi|card exchange|"
    r"guruhimizga|xush|kelibsiz|shirinliklar|mahallasi|registratsiyadan|kuniga|"
    r"tungi|tanishuv|tanishuvlar|taniwivla|bevalar|suxbat|suhbat|guruh|guruhi|ulaymiz|ulemiz|ming|soat|narx|narxi|narxlarda|"
    r"dostavka|viloyat|toshkent|fargona|xorazm|samarqand|andijon|namangan|buxoro|qarshi|termiz|navoiy|olmaliq|"
    r"uchun|bilan|yangi|yoshlari|alqi|satqi|olmaz|gaydalari|elanim|alver|guvenli|ishlab|chiqaruvchidan|arzon|sifatli|mebel|"
    r"resmi|grubudur|yuksek|derece|balina|icerir|turkiye|sohbet|yardimlasma|arayis|"
    r"lpm|roleplayer|roleplay|hastle|kena|unmute di|halo|cantik|kamu|selama|karena|belum|semua|patut|hantam|dihous|sahabat|cerita|bagi|bocah|bokep|pinoy|filipino|pilipinas|mura|babi|kontol|memek|jembut|asw|bangsat|tolol|goblok|jual|beli|jasa|sewa|gratisan|dana|pulsa|kuota|slot|zeus|pragmatic|maxwin|menfess|fess|rprl|fambest|"
    r"gacor|slot gacor|asupan|bocil|indo|jakarta|bandung|selamat|bergabung|cari|brondong|sayang|"
    r"diskusi|uang|berisi|informasi|cara|menghasilkan|rekomendasi|jadi|adalah|bebas|membahas|seperti|mutualan|"
    r"berbasis|terkoneksi|jualan|murah|silahkan|berbagi|teman|pacar|ngobrol|kenalan|"
    r"konten|indo viral|viral indo|pinay|kantutan|alap|bhabhi|desi|tamil|telugu|hindi|mallu|kerala|"
    r"weltweit|gruppe|alles|rund|deutsch|deutschland|polska|polski|czat|swiat|kryptowalut|"
    r"vous|soyez|concu|supplements|europe|bienvenue|francais|italiano|gruppo|sotto|esiste|neanche|dopo|imparare|insegnare|"
    r"portugal|lisboa|porto|tuga|tugas|angola|luanda|mocambique|mozambique|maputo|cabo verde|telemovel|autocarro|ecra|generos"
    r")\b",
    re.I
)

RE_INDICADOR_BR = re.compile(
    r"\b("
    r"brasil|brasileiro|brasileira|brasileiros|brasileiras|"
    r"amizade|amizades|namoro|paquera|bate-papo|bate papo|conversa|conversas|conversar|resenha|zoeira|zueira|"
    r"galera|pessoal|bem-vindo|bem vindo|bem-vindos|bem vindos|seja|sejam|regras|proibido|respeito|"
    r"voce|voces|nao|entao|tambem|aqui|isso|isto|aquilo|sao|estao|falar|fazer|feito|criado|destinado|"
    r"troca|conhecimento|conhecimentos|experiencia|experiencias|duvidas|ajuda|assuntos|usuarios|"
    r"interagir|interacao|discussao|discussoes|debates|torcedores|apaixonados|iniciantes|avancados|"
    r"concurso|concursos|questoes|materiais|estudo|estudos|estudar|estudantes|profissionais|pesquisas|"
    r"enem|vestibular|vestibulares|faculdade|universidade|professor|professores|alunos|"
    r"futebol|brasileirao|corinthians|flamengo|flamenguista|palmeiras|santos|gremio|cruzeiro|vasco|botafogo|fluminense|"
    r"paulista|carioca|mineiro|gaucho|baiano|pernambucano|cearense|paranaense|catarinense|goiano|capixaba|nordestina|nordestino|"
    r"sao paulo|rio de janeiro|belo horizonte|porto alegre|curitiba|florianopolis|salvador|recife|fortaleza|manaus|belem|goiania|brasilia|"
    r"maceio|aracaju|rio branco|boa vista|sao luis|teresina|natal|joao pessoa|cuiaba|campo grande|macapa|palmas|porto velho|vitoria|"
    r"campinas|guarulhos|sao bernardo|santo andre|osasco|sorocaba|ribeirao preto|sao jose dos campos|"
    r"uberlandia|contagem|juiz de fora|niteroi|duque de caxias|nova iguacu|sao goncalo|petropolis|"
    r"londrina|maringa|ponta grossa|cascavel|foz do iguacu|joinville|blumenau|chapeco|criciuma|itajai|"
    r"caxias do sul|pelotas|canoas|santa maria|gravatai|feira de santana|vitoria da conquista|juazeiro|olinda|caruaru|petrolina|"
    r"aparecida de goiania|anapolis|itumbiara|ananindeua|santarem|maraba|vila velha|campina grande|mossoro|arapiraca|dourados|aripuana|"
    r"amazonas|ceara|paraiba|pernambuco|bahia|minas gerais|parana|santa catarina|rio grande do sul|mato grosso|espirito santo|piaui|maranhao|alagoas|sergipe|rondonia|roraima|amapa|tocantins|"
    r"membros|administrador|privado|comunidade|oficina|turma|"
    r"filmes|series|dublado|legendado|musicas|jogos|livros|leitura|poesia|"
    r"politica|direita|esquerda|bolsonaro|lula|patriotas|cristao|evangelico|catolico|espirita|umbanda|ateismo|bruxaria|"
    r"programacao|desenvolvedores|tecnologia|informatica|computadores|computacao|forense|pericia|virtualizacao|"
    r"contabilidade|direito|engenharia|arquitetura|medicina|enfermagem|psicologia|nutricao|educacao|historia|geografia|matematica|fisica|quimica|biologia|"
    r"compra e venda|compras e vendas|alugueis|imoveis|oportunidades|emprego|empregos|"
    r"carros|motos|caminhao|onibus|ciclismo|corrida|academia|treino|musculacao|saude|receitas|culinaria|plantas|cachorros|gatos|"
    r"figurinhas|comedia|fofoca|babados|noticias|atualidades|divulgacao|previas|novinhos|novinhas|coroas"
    r")\b",
    re.I
)

RE_ESTRUTURA_PT = re.compile(
    r"\b("
    r"do|da|dos|das|na|nas|um|uma|uns|umas|ao|aos|pelo|pela|pelos|pelas|"
    r"voce|voces|nao|entao|tambem|aqui|isso|isto|aquilo|sao|estao|seja|sejam|"
    r"muito|mais|ninguem|alguem|outro|outra|outros|outras|nosso|nossa|nossos|nossas|"
    r"pra|pro|vc|vcs|tb|tbm|pq|blz|glr|ngm|cmg|ctg|flw|vlw"
    r")\b",
    re.I
)

RE_ACENTOS_PT = re.compile(r"[ãçêÃÇÊ]")

RE_ANTISPAM = re.compile(
    r"(rose|missrose|captcha|shieldy|combot|grouphelp|protectron|bitcap|safeguard|chatkeeper|guardbot|securitybot|verifiq|verificacao|verificação|provar que voce|provar que você|humano|aperta o botao|aperte o botão|bot.*admin|anti-spam|antispam|welcome.*bot)",
    re.I
)

BANIDOS_CONFIRMADOS = {
    "https://t.me/+XpLqhA0_K6RhZTYx",
    "https://t.me/+UsbeM2uPMp45N2Fh",
    "https://t.me/tuto4all",
    "https://t.me/oishaxonimtortik",
    "https://t.me/tvxiaomiadictos",
    "https://t.me/fasihacakes",
    "https://t.me/grupodeajudabrasileirosnaespanha",
    "https://t.me/prolooteroffical",
    "https://t.me/bengaliservicehub",
    "https://t.me/xiaomi9lite",
    "https://t.me/metaearthmozambique",
    "https://t.me/+G0KkBFeBuyol4U6cs7wTLw",
    "https://t.me/jetlatam",
    "https://t.me/loootssecretlooters07",
    "https://t.me/brasileportugalamizade",
    "https://t.me/dunyo00111",
    "https://t.me/kriptoanalizpro",
    "https://t.me/ochatpt",
    "https://t.me/+H5wrz0YvJzdiZTVk",
    "https://t.me/conversafiadabr",
}

def scrape_url_http(url: str):
    """Scrapes public telegram group page to grab title, members, desc, image."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code != 200:
            return url, None
        txt = r.text
        mt = RE_TITLE.search(txt)
        me = RE_EXTRA.search(txt)
        if not mt or not me:
            return url, None
        titulo = clean_html(mt.group(1))
        extra = clean_html(me.group(1))
        extra_low = extra.lower()
        if "member" not in extra_low and "membro" not in extra_low:
            return url, None
        membros = parse_membros(extra)
        if membros < 5:
            return url, None
        md = RE_DESC.search(txt)
        desc = clean_html(md.group(1)) if md else ""
        mi = RE_IMG.search(txt)
        imagem = mi.group(1).strip() if mi else ""
        return url, {
            "url": url,
            "valido": True,
            "status": "br",
            "titulo": titulo,
            "membros": membros,
            "extra": extra,
            "desc": desc,
            "imagem": imagem
        }
    except Exception:
        return url, None

def avaliar_grupo(g: dict, expirados_set: set, sem_chat_set: set) -> tuple:
    """Aplica as 3 regras de moderação estrita. Retorna (True, dados_formatados) ou (False, motivo)."""
    url = canon_url(g.get("url") or "")
    k = url_key(url)
    titulo = (g.get("titulo") or "").strip()
    desc = (g.get("desc") or "").strip()
    extra = (g.get("extra") or "").strip()
    membros = g.get("membros") or 0
    imagem = g.get("imagem") or ""

    # Regra 0: Apenas links públicos definitivos (sem hash privado que expira)
    if "+" in url or "joinchat" in url:
        return False, "link_privado_expirado"

    # Regra 1: Sem expirados / inativos (mínimo 15 membros)
    if k in expirados_set or membros < 15 or not titulo:
        return False, "expirado_ou_inativo"
    if not re.match(r"^https://t\.me/[a-zA-Z0-9_]{3,}$", url):
        return False, "expirado_ou_inativo"

    # Regra 2: Com chat aberto e ZERO CANAIS
    extra_low = extra.lower()
    if k in sem_chat_set:
        return False, "sem_chat"
    
    # Se Telegram diz subscriber/inscrito, É CANAL
    if "subscriber" in extra_low or "inscrito" in extra_low:
        return False, "canal"
    # Se tem extra e não comprova membros de grupo aberto, descarta
    if extra and not any(m in extra_low for m in ["member", "membro", "online"]):
        return False, "canal"

    # Proibir palavras de canal no título
    if re.search(r"\b(canal|channel|apenas canal|canal oficial|inscreva-se|inscritos|notícias oficiais)\b", titulo, re.I):
        return False, "canal"

    texto_orig = f"{titulo} {desc}"
    texto_sem_url = strip_urls(texto_orig)
    texto_norm = norm_text(f"{url} {texto_sem_url}")

    if RE_SEM_CHAT.search(texto_norm):
        return False, "sem_chat"

    # Regra 3: 100% Brasileiro
    if RE_SCRIPT_GRINGO.search(texto_orig) or tem_bandeira_gringa(texto_orig):
        return False, "gringo"

    texto_busca = norm_text(texto_sem_url)
    matches_gringo = RE_GRINGO.findall(texto_busca)
    matches_pt_est = RE_ESTRUTURA_PT.findall(texto_busca)
    matches_br_ind = RE_INDICADOR_BR.findall(texto_busca)
    tem_acento_pt = bool(RE_ACENTOS_PT.search(texto_orig)) or "🇧🇷" in texto_orig

    if re.search(r"\b[a-z]+\s+y\s+[a-z]+\b", texto_busca):
        if len(matches_pt_est) < 4:
            return False, "gringo"

    if len(matches_gringo) >= 1:
        if len(matches_pt_est) < len(matches_gringo) * 4 or len(matches_pt_est) < 3:
            return False, "gringo"

    e_br = False
    if "🇧🇷" in texto_orig and len(matches_gringo) == 0:
        e_br = True
    elif tem_acento_pt and len(matches_gringo) == 0:
        # Tem acentos do português (ã, ç, ê etc.) e nenhuma palavra gringa: é BR
        e_br = True
    elif tem_acento_pt and (len(matches_br_ind) >= 1 or len(matches_pt_est) >= 1):
        e_br = True
    elif len(matches_pt_est) >= 2 and len(matches_br_ind) >= 1:
        e_br = True
    elif len(matches_br_ind) >= 2 and len(matches_pt_est) >= 1:
        e_br = True
    elif len(matches_br_ind) >= 2 and len(matches_gringo) == 0:
        e_br = True
    elif len(matches_br_ind) >= 1 and len(matches_gringo) == 0 and len(matches_pt_est) >= 1:
        e_br = True
    elif len(matches_gringo) == 0 and len(matches_pt_est) >= 2:
        # Nenhuma palavra gringa + 2+ estruturas PT claras (do, da, das, nao, voce...)
        e_br = True
    elif len(matches_gringo) == 0 and len(matches_pt_est) >= 1 and (tem_acento_pt or len(matches_br_ind) >= 1):
        # Exige pelo menos indício PT e acentuação ou estado brasileiro
        e_br = True

    if not e_br:
        return False, "sem_indicio_br"

    requer_captcha = bool(RE_ANTISPAM.search(texto_busca))
    return True, {
        "url": url,
        "titulo": titulo,
        "membros": membros,
        "desc": desc[:200],
        "imagem": imagem,
        "requer_captcha": requer_captcha,
        "tipo": "antispam" if requer_captcha else "livre"
    }

def processar_e_adicionar_links(texto_links: str) -> dict:
    """Extrai links, valida via HTTP os novos, aplica as 3 regras, atualiza br_validados.json e index.html."""
    raw_found = RE_LINK.findall(texto_links)
    links_normalizados = []
    vistos_input = set()
    for m in raw_found:
        u = canon_url(m)
        uk = url_key(u)
        if uk not in vistos_input and not u.startswith("https://t.me/c/"):
            vistos_input.add(uk)
            links_normalizados.append(u)

    total_recebidos = len(links_normalizados)
    if total_recebidos == 0:
        return {
            "sucesso": False,
            "erro": "Nenhum link do Telegram (t.me/...) foi encontrado no texto fornecido."
        }

    # Carregar dados locais
    sem_chat_set = {url_key(u) for u in BANIDOS_CONFIRMADOS}
    for cf in [DESCARTADOS_TXT, os.path.join(BASE_DIR, "dados", "canais.txt"), os.path.join(BASE_DIR, "dados", "validos_canais.txt")]:
        if os.path.exists(cf):
            with open(cf, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    s = line.strip().split('#')[0].strip()
                    if s:
                        sem_chat_set.add(url_key(s))

    expirados_set = set()
    if os.path.exists(EXPIRADOS_TXT):
        with open(EXPIRADOS_TXT, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if s:
                    expirados_set.add(url_key(s))

    coleta_dict = {}
    if os.path.exists(COLETA_JSONL):
        with open(COLETA_JSONL, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        d = json.loads(line)
                        u = d.get("url")
                        if u:
                            coleta_dict[url_key(u)] = d
                    except Exception:
                        pass

    # Descobrir quais precisam de scrape HTTP
    missing = []
    for u in links_normalizados:
        uk = url_key(u)
        if uk not in coleta_dict and uk not in expirados_set:
            missing.append(u)

    novos_validos = []
    novos_expirados = []
    if missing:
        with ThreadPoolExecutor(max_workers=min(40, len(missing))) as ex:
            futs = {ex.submit(scrape_url_http, u): u for u in missing}
            for fut in as_completed(futs):
                u, res = fut.result()
                if res and isinstance(res, dict):
                    novos_validos.append(res)
                    coleta_dict[url_key(u)] = res
                else:
                    novos_expirados.append(u)
                    expirados_set.add(url_key(u))

        # Salvar novos raspados
        if novos_validos:
            with open(COLETA_JSONL, "a", encoding="utf-8") as f:
                for item in novos_validos:
                    f.write(json.dumps(item, ensure_ascii=False) + "\n")

        if novos_expirados:
            with open(EXPIRADOS_TXT, "a", encoding="utf-8") as f:
                for u in novos_expirados:
                    f.write(u + "\n")

    # Carregar br_validados.json atual
    aprovados_existentes = []
    if os.path.exists(BR_JSON):
        try:
            with open(BR_JSON, "r", encoding="utf-8") as f:
                aprovados_existentes = json.load(f)
        except Exception:
            aprovados_existentes = []

    urls_existentes = {url_key(g["url"]) for g in aprovados_existentes}
    titulos_existentes = {re.sub(r"[^a-z0-9]", "", norm_text(g["titulo"])) for g in aprovados_existentes if g.get("titulo")}

    novos_aprovados = []
    motivos_descarte = {
        "expirado_ou_inativo": 0,
        "sem_chat": 0,
        "canal": 0,
        "gringo": 0,
        "sem_indicio_br": 0,
        "duplicado": 0
    }

    for u in links_normalizados:
        uk = url_key(u)
        if uk in urls_existentes:
            motivos_descarte["duplicado"] += 1
            continue

        g = coleta_dict.get(uk)
        if not g:
            motivos_descarte["expirado_ou_inativo"] += 1
            continue

        ok, resultado = avaliar_grupo(g, expirados_set, sem_chat_set)
        if not ok:
            motivos_descarte[resultado] = motivos_descarte.get(resultado, 0) + 1
            continue

        # Verificar duplicado de título
        tn = re.sub(r"[^a-z0-9]", "", norm_text(resultado["titulo"]))
        if tn in titulos_existentes:
            motivos_descarte["duplicado"] += 1
            continue

        urls_existentes.add(uk)
        if tn:
            titulos_existentes.add(tn)
        novos_aprovados.append(resultado)

    # Mesclar e ordenar
    todos_aprovados = aprovados_existentes + novos_aprovados
    todos_aprovados.sort(key=lambda x: -(x.get("membros") or 0))

    # Salvar br_validados.json
    with open(BR_JSON, "w", encoding="utf-8") as f:
        json.dump(todos_aprovados, f, ensure_ascii=False, indent=2)

    # Atualizar o site estático resultados/site/index.html se existir gerar_site
    try:
        import gerar_site
        gerar_site.gerar_html(todos_aprovados, parcial=False)
    except Exception as e:
        print(f"Aviso ao gerar site estático: {e}")

    return {
        "sucesso": True,
        "total_recebidos": total_recebidos,
        "novos_analisados": len(missing),
        "novos_aprovados": len(novos_aprovados),
        "total_atual_site": len(todos_aprovados),
        "descartados_expirados": motivos_descarte["expirado_ou_inativo"],
        "descartados_sem_chat": motivos_descarte["sem_chat"] + motivos_descarte["canal"],
        "descartados_gringos": motivos_descarte["gringo"] + motivos_descarte["sem_indicio_br"],
        "duplicados": motivos_descarte["duplicado"],
        "amostra_novos": [{"titulo": g["titulo"], "membros": g["membros"], "url": g["url"]} for g in novos_aprovados[:10]]
    }
