# -*- coding: utf-8 -*-
"""
pipeline/extract/extractor.py
Módulo responsável por extrair, limpar e normalizar links do Telegram a partir de qualquer texto, HTML ou URL bruta.
"""

import html
import re
import urllib.parse
from typing import Optional, Set, Tuple

# Regex abrangente para detectar links do Telegram em múltiplos formatos
RE_TG_URLS = re.compile(
    r"""(?x)
    (?:https?://)?
    (?:www\.)?
    (?:
        (?:t|telegram)\.me/
        |telegram\.dog/
    )
    (?P<target>(?:\+|joinchat/)?[a-zA-Z0-9_+\-]+(?:/[0-9]+)?)
    |
    tg://resolve\?domain=(?P<domain>[a-zA-Z0-9_]+)
    |
    tg://join\?invite=(?P<invite>[a-zA-Z0-9_\-]+)
    """,
    re.IGNORECASE,
)

# Usernames de sistema, serviços, bots ou utilitários que nunca são grupos
IGNORAR_USERNAMES = {
    "share", "telegram", "telegramorg", "whatsapptelegramgrupos", "telegrupos18bot",
    "listatelegruposbot", "pluslistasbot", "suporte", "parceriatelegrabot",
    "metricgramofficial", "addstickers", "iv", "socks", "proxy", "login",
    "signup", "setlanguage", "joinchat", "c", "s", "bot", "stickers",
    "spambot", "terms", "faq", "jobs", "durov", "contest"
}


def canonicalize_url(raw_url: str) -> Optional[Tuple[str, Optional[str], str]]:
    """
    Normaliza uma URL do Telegram para o formato padrão.
    Retorna uma tupla: (canonical_url, username_ou_none, dedup_key)
    ou None se o link for inválido ou pertencer a um serviço reservado.
    """
    if not raw_url:
        return None

    u = raw_url.strip()
    try:
        u = urllib.parse.unquote(u)
    except Exception:
        pass

    # Tratar esquemas tg://
    if u.lower().startswith("tg://resolve?domain="):
        param = u.split("=", 1)[-1].strip()
        u = f"https://t.me/{param}"
    elif u.lower().startswith("tg://join?invite="):
        param = u.split("=", 1)[-1].strip()
        u = f"https://t.me/+{param}"

    # Padronizar domínio e protocolo
    u = u.replace("http://", "https://")
    u = re.sub(r"^https?://(?:www\.)?telegram\.me/", "https://t.me/", u, flags=re.I)
    u = re.sub(r"^https?://(?:www\.)?telegram\.dog/", "https://t.me/", u, flags=re.I)

    if not u.startswith("https://t.me/"):
        if u.startswith("t.me/"):
            u = "https://" + u
        else:
            return None

    # Remover query parameters e fragmentos
    u = u.split("?")[0].split("#")[0].rstrip("/")

    # Remover sufixos numéricos de mensagens (ex: t.me/grupo/1234 -> t.me/grupo)
    # exceto se for canal privado com /c/123... que não é grupo público
    if "/c/" in u:
        return None

    m_msg = re.match(r"^(https://t\.me/[a-zA-Z0-9_+]+)/[0-9]+$", u)
    if m_msg:
        u = m_msg.group(1)

    # Padronizar joinchat/HASH para +HASH
    m_join = re.match(r"^https://t\.me/joinchat/([a-zA-Z0-9_\-]+)$", u, re.I)
    if m_join:
        u = f"https://t.me/+{m_join.group(1)}"

    # Validar formato final
    # Grupos públicos têm https://t.me/username
    # Grupos com convite têm https://t.me/+hash
    m_valid = re.match(r"^https://t\.me/(\+?[a-zA-Z0-9_\-]{3,})$", u)
    if not m_valid:
        return None

    slug = m_valid.group(1)
    if slug.startswith("+"):
        invite_hash = slug[1:]
        if len(invite_hash) < 5:
            return None
        canon = f"https://t.me/+{invite_hash}"
        # Hashes de convite são case-sensitive
        return canon, None, canon
    else:
        username = slug.lower()
        if username in IGNORAR_USERNAMES:
            return None
        # Descartar bots óbvios (terminam em 'bot', exceto termos como 'robot' ou 'grupocba')
        if username.endswith("bot") and username not in {"robot", "grupocba"}:
            return None
        canon = f"https://t.me/{slug}"
        # Usernames são case-insensitive
        return canon, slug, f"https://t.me/{username}"


def extract_telegram_links(text: str) -> Set[str]:
    """
    Extrai todos os links canônicos do Telegram encontrados em um bloco de texto ou HTML.
    """
    if not text:
        return set()

    # Desescapar entidades HTML e URL
    txt = html.unescape(text)
    try:
        txt_unq = urllib.parse.unquote(txt)
    except Exception:
        txt_unq = ""

    conteudo = txt + "\n" + txt_unq
    encontrados = set()

    for match in RE_TG_URLS.finditer(conteudo):
        target = match.group("target")
        domain = match.group("domain")
        invite = match.group("invite")

        raw_match = ""
        if target:
            raw_match = f"https://t.me/{target}"
        elif domain:
            raw_match = f"https://t.me/{domain}"
        elif invite:
            raw_match = f"https://t.me/+{invite}"

        res = canonicalize_url(raw_match)
        if res:
            canon, _, _ = res
            encontrados.add(canon)

    return encontrados
