# -*- coding: utf-8 -*-
"""
pipeline/validate/validator.py
Validador de links públicos do Telegram.
Aplica os filtros estritos de:
1. Tipo (somente grupos REAIS, descartando canais, murais e bots);
2. Validade (apenas links ativos e existentes);
4. Segurança (descarte de conteúdo ilegal).
"""

import asyncio
import html
import random
import re
from typing import Any, Dict, Optional, Tuple
import aiohttp
from bs4 import BeautifulSoup

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/125.0.0.0 Safari/537.36",
]

RE_EXTRA_MEMBERS = re.compile(r"(\d[\d\s\.,]*)\s*(?:members?|membros?)", re.IGNORECASE)
RE_EXTRA_SUBSCRIBERS = re.compile(r"(\d[\d\s\.,]*)\s*(?:subscribers?|inscritos?)", re.IGNORECASE)

# Termos que identificam inequivocamente um canal unidirecional ou mural fechado
RE_CANAL_FATAL = re.compile(
    r"\b("
    r"canal|channel|apenas canal|somente canal|canal oficial|canal pessoal|nosso canal|"
    r"canal de noticias|canal de notícias|canal de ofertas|canal de cupons|canal de vagas|"
    r"canal de filmes|canal de series|canal de apostas|canal vip|canal free|"
    r"inscreva-se no canal|inscrevam-se no canal|se inscreva no canal|"
    r"subscribers|inscritos|apenas avisos|somente avisos|mural de avisos|mural de ofertas|"
    r"feed oficial|avisos e regras|sinais oficiais|apenas administradores|somente administradores|"
    r"chat fechado|chat bloqueado|chat travado|somente leitura|apenas leitura"
    r")\b",
    re.IGNORECASE,
)

# Termos de conteúdo estritamente ilegal (Segurança - Filtro 4)
RE_ILLEGAL_CONTENT = re.compile(
    r"("
    # Exploração sexual infantil / CSAM / Menores
    r"\b(cp|pedofilia|pedofilo|pedofilos|child\s*porn|novinhas\s*vazadas\s*menores|menor\s*de\s*idade|criancinhas|vazados\s*infantis)\b|"
    # Tráfico de drogas pesadas e armas ilícitas
    r"\b(venda\s*de\s*armas?|comprar\s*arma|fuzil\s*a\s*venda|pistola\s*raspada|venda\s*de\s*cocaina|entrega\s*de\s*drogas|maconha\s*delivery\s*brasil|droga\s*sintetica\s*venda)\b|"
    # Fraude financeira severa / Estelionato / Carding / Dados vazados
    r"\b(clonagem\s*de\s*cartao|cartoes\s*clonados|venda\s*de\s*cc|cc\s*full|notas\s*falsas\s*venda|notas\s*fakes|dinheiro\s*falso\s*envio|puxada\s*de\s*dados\s*serasa|painel\s*puxa\s*dados)\b|"
    # Violência extrema / Terrorismo / Gore pesado
    r"\b(snuff|decapitacao|tortura\s*real|massacre\s*ao\s*vivo|suicidio\s*ao\s*vivo)\b"
    r")",
    re.IGNORECASE,
)


def parse_clean_number(text: str) -> int:
    """Extrai número inteiro de string formatada (ex: '118 453' ou '1.250')."""
    if not text:
        return 0
    clean = re.sub(r"[^\d]", "", text)
    return int(clean) if clean else 0


def parse_telegram_preview_html(url: str, html_text: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Analisa o HTML da página do Telegram e aplica com rigor absoluto os filtros
    de Tipo (Somente Grupos/Chats), Validade e Segurança.
    """
    if not html_text or len(html_text) < 50:
        return False, "expirado", None

    raw_lower = html_text.lower()
    for msg in ["invite link is expired", "link de convite expirado", "the chat does not exist", "channel not found", "group not found"]:
        if msg in raw_lower:
            return False, "expirado", None

    soup = BeautifulSoup(html_text, "html.parser")

    title_el = soup.find("div", class_="tgme_page_title")
    extra_el = soup.find("div", class_="tgme_page_extra")
    desc_el = soup.find("div", class_="tgme_page_description")
    action_el = soup.find("a", class_=re.compile(r"tgme_action_button"))
    img_el = soup.find("img", class_="tgme_page_photo_image")

    nome = title_el.get_text(strip=True) if title_el else ""
    extra = extra_el.get_text(strip=True) if extra_el else ""
    desc = desc_el.get_text(strip=True) if desc_el else ""
    action_text = action_el.get_text(strip=True) if action_el else ""
    imagem = img_el["src"].strip() if img_el and img_el.has_attr("src") else ""

    # FILTRO 2: Validade
    if not nome:
        return False, "expirado", None

    extra_lower = extra.lower()
    action_lower = action_text.lower()

    # 1. BOT OU PERFIL PESSOAL: botão "Send Message"
    if "send message" in action_lower or "enviar mensagem" in action_lower:
        return False, "bot_ou_usuario", None

    # FILTRO 1: Tipo (Apenas Grupos/Chats - Eliminação Implacável de Canais)
    # 2. Se extra tem "subscriber" ou "inscrito" -> É CANAL
    if "subscriber" in extra_lower or "inscrito" in extra_lower:
        return False, "canal", None

    # 3. Se botão tem "Join Channel" ou "Inscrever-se" -> É CANAL
    if "join channel" in action_lower or "inscrever-se" in action_lower or "entrar no canal" in action_lower:
        return False, "canal", None

    # 4. CRITÉRIO OBRIGATÓRIO DE GRUPO:
    # Deve comprovar contagem de "member"/"membro" no extra OU botão "Join Group" / "Entrar no Grupo"
    m_mem = RE_EXTRA_MEMBERS.search(extra_lower)
    tem_membros = bool(m_mem) or ("member" in extra_lower or "membro" in extra_lower)
    tem_botao_grupo = "join group" in action_lower or "entrar no grupo" in action_lower

    if not tem_membros and not tem_botao_grupo:
        # Sem comprovação inequívoca de membros ou chat, descarta como canal/inválido
        return False, "canal", None

    membros = 0
    if m_mem:
        membros = parse_clean_number(m_mem.group(1))
    elif tem_membros:
        membros = parse_clean_number(extra.split(",")[0])
    elif tem_botao_grupo:
        membros = 10  # Grupo privado ativo com prévia oculta

    if membros < 5 and not tem_botao_grupo:
        return False, "expirado", None

    # 5. Regra Estrita: Se o título declara canal, ofertas ou avisos, descarte imediatamente
    texto_completo = f"{nome} {desc}".lower()
    if RE_CANAL_FATAL.search(nome):
        return False, "canal", None
    if RE_CANAL_FATAL.search(desc) and not ("grupo de bate-papo" in texto_completo or "chat da comunidade" in texto_completo):
        return False, "canal", None

    # FILTRO 4: Segurança (Descarte de conteúdo ilegal)
    if RE_ILLEGAL_CONTENT.search(nome) or RE_ILLEGAL_CONTENT.search(desc):
        return False, "ilegal", None

    dados = {
        "link_convite": url,
        "nome": nome,
        "descricao": desc,
        "membros": membros,
        "tipo": "grupo",
        "imagem": imagem,
        "extra_info": extra,
    }
    return True, "valido", dados


async def validate_telegram_url(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int = 2,
    timeout_sec: float = 7.0,
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Realiza GET HTTP assíncrono na página pública t.me com rotação de headers e backoff.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    }

    delay = 0.8
    for attempt in range(max_retries):
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout_sec)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    return parse_telegram_preview_html(url, text)
                elif resp.status in (429, 503, 502):
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
                elif resp.status in (404, 410):
                    return False, "expirado", None
                else:
                    return False, "expirado", None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt < max_retries - 1:
                await asyncio.sleep(delay)
            else:
                return False, "falha_rede", None
        except Exception:
            return False, "falha_desconhecida", None

    return False, "expirado", None
