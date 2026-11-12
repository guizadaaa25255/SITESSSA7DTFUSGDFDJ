"""
dashboard.py
============
Servidor Flask local para o Dashboard de Grupos BR do Telegram.

Uso:
    python dashboard.py

Abre no navegador: http://localhost:5000
"""

import asyncio
import json
import logging
import os
import queue
import random
import re
import sys
import threading
import time
from datetime import date
from functools import wraps

from flask import Flask, Response, jsonify, render_template, request, session

# ── Telethon ────────────────────────────────────────────────────────────────
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest, LeaveChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetFullChatRequest
from telethon.tl.types import Channel, Chat, ChannelParticipantBanned
from telethon.errors import (
    FloodWaitError, UserAlreadyParticipantError, InviteHashExpiredError,
    InviteRequestSentError, RPCError, SessionPasswordNeededError,
    UsernameNotOccupiedError, UsernameInvalidError
)

from pipeline.classify.brazil_classifier import check_brazil_confidence

# ── Paths ────────────────────────────────────────────────────────────────────
BASE      = os.path.dirname(os.path.abspath(__file__))
DADOS     = os.path.join(BASE, "dados")
RESULT    = os.path.join(BASE, "resultados")
CONF_ARQ  = os.path.join(BASE, "dashboard_app", "config.json")
SESS_ARQ  = os.path.join(DADOS, "sessao_dashboard")   # sem .session
GRUPOS_ARQ = os.path.join(RESULT, "br_validados.json")
ESTADO_ARQ = os.path.join(DADOS, "estado_dashboard.json")
FEITOS_ARQ = os.path.join(DADOS, "ja_entrou_dashboard.txt")
JA_PROC_ARQ = os.path.join(DADOS, "ja_processados.txt")

os.makedirs(DADOS, exist_ok=True)
os.makedirs(RESULT, exist_ok=True)

# ── Flask ────────────────────────────────────────────────────────────────────
logger = logging.getLogger("dashboard")

app = Flask(__name__,
            template_folder="dashboard_app/templates",
            static_folder="dashboard_app/static")
app.secret_key = os.urandom(24)

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response

# ── Estado global do cliente Telethon ────────────────────────────────────────
_client: TelegramClient = None
_clients_pool: dict = {}  # clean_phone -> TelegramClient persistente
_loop:   asyncio.AbstractEventLoop = None
_thread: threading.Thread = None
_phone_code_hash = None
_client_lock = threading.RLock()
_locks_por_conta: dict = {}  # clean_phone -> threading.RLock() para paralelismo real entre abas

def obter_lock_conta(phone=None):
    clean = re.sub(r"[^0-9]", "", str(phone or ""))
    if not clean:
        return _client_lock
    with _client_lock:
        if clean not in _locks_por_conta:
            _locks_por_conta[clean] = threading.RLock()
        return _locks_por_conta[clean]

# ── Auto-join state ───────────────────────────────────────────────────────────
_autojoin_running  = False
_autojoin_stop     = threading.Event()
_autojoin_queue: queue.Queue = queue.Queue()   # SSE log messages
_autojoin_thread: threading.Thread = None

# ── Broadcast (Disparador de mensagens) state ─────────────────────────────────
_broadcast_running = False
_broadcast_stop    = threading.Event()
_broadcast_queue: queue.Queue = queue.Queue()
_broadcast_thread: threading.Thread = None


# ══════════════════════════════════════════════════════════════════════════════
# Configuração (api_id / api_hash / phone)
# ══════════════════════════════════════════════════════════════════════════════

def carregar_conf():
    if os.path.exists(CONF_ARQ):
        try:
            with open(CONF_ARQ, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def salvar_conf(d):
    os.makedirs(os.path.dirname(CONF_ARQ), exist_ok=True)
    with open(CONF_ARQ, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
# Loop asyncio em thread dedicada
# ══════════════════════════════════════════════════════════════════════════════

def _iniciar_loop():
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    _loop.run_forever()


def run_async(coro):
    """Executa corrotina no loop dedicado e retorna o resultado."""
    if _loop is None:
        raise RuntimeError("Loop não iniciado")
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=60)


def garantir_loop():
    global _thread
    if _thread is None or not _thread.is_alive():
        _thread = threading.Thread(target=_iniciar_loop, daemon=True)
        _thread.start()
        time.sleep(0.3)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers Telethon
# ══════════════════════════════════════════════════════════════════════════════

def get_client() -> TelegramClient:
    return _client


def cliente_conectado():
    return _client is not None and _client.is_connected()


def _obter_sessao_path(phone=None):
    if phone:
        clean = re.sub(r"[^0-9]", "", str(phone))
        if clean:
            return os.path.join(DADOS, f"sessao_{clean}")
    conf = carregar_conf()
    cfg_phone = conf.get("phone")
    if cfg_phone:
        clean = re.sub(r"[^0-9]", "", str(cfg_phone))
        if clean:
            p = os.path.join(DADOS, f"sessao_{clean}")
            if os.path.exists(p + ".session"):
                return p
    return SESS_ARQ


async def _criar_cliente(api_id, api_hash, phone=None):
    global _client, _clients_pool
    clean = re.sub(r"[^0-9]", "", str(phone or ""))
    if clean and clean in _clients_pool:
        c = _clients_pool[clean]
        try:
            if not c.is_connected():
                await c.connect()
            _client = c
            return c
        except Exception:
            _clients_pool.pop(clean, None)

    sess_path = _obter_sessao_path(phone)
    try:
        c = TelegramClient(sess_path, int(api_id), api_hash, loop=_loop)
        await c.connect()
    except Exception as e:
        if "locked" in str(e).lower() and os.path.exists(sess_path + ".session"):
            import shutil
            rt_path = os.path.join(DADOS, f"sessao_rt_{clean or 'default'}")
            try:
                shutil.copy2(sess_path + ".session", rt_path + ".session")
            except Exception:
                pass
            c = TelegramClient(rt_path, int(api_id), api_hash, loop=_loop)
            await c.connect()
        else:
            raise

    if clean:
        _clients_pool[clean] = c
    _client = c
    return c


async def _grupos_que_participo():
    """Retorna set de usernames/hashes que o usuário já está."""
    membros = set()
    async for dialog in _client.iter_dialogs():
        e = dialog.entity
        username = getattr(e, "username", None)
        if username:
            membros.add(username.lower())
        membros.add(str(getattr(e, "id", "")))
    return membros


def normalizar_link(url):
    url = url.strip()
    m = re.search(r"t\.me/\+([A-Za-z0-9_-]+)", url)
    if m:
        return "invite", m.group(1)
    m = re.search(r"t\.me/joinchat/([A-Za-z0-9_-]+)", url)
    if m:
        return "invite", m.group(1)
    m = re.search(r"t\.me/([A-Za-z0-9_]{4,})", url)
    if m:
        return "username", m.group(1)
    return None, None


# ══════════════════════════════════════════════════════════════════════════════
# Estado diário (limite de entradas)
# ══════════════════════════════════════════════════════════════════════════════

def _obter_telefone_ativo(phone=None):
    if phone:
        c = re.sub(r"[^0-9]", "", str(phone))
        if c:
            return c
    conf = carregar_conf()
    p = conf.get("phone", "")
    return re.sub(r"[^0-9]", "", p)

def _obter_arquivos_conta(phone=None):
    clean = _obter_telefone_ativo(phone)
    if clean:
        feitos = os.path.join(DADOS, f"ja_entrou_{clean}.txt")
        estado = os.path.join(DADOS, f"estado_{clean}.json")
    else:
        feitos = os.path.join(DADOS, "ja_entrou_dashboard.txt")
        estado = os.path.join(DADOS, "estado_dashboard.json")
    return feitos, estado

def carregar_estado(phone=None):
    _, estado_arq = _obter_arquivos_conta(phone)
    try:
        with open(estado_arq, encoding="utf-8") as f:
            e = json.load(f)
        if e.get("data") == str(date.today()):
            return e.get("entradas", 0)
    except Exception:
        pass
    return 0


def salvar_estado(n, phone=None):
    _, estado_arq = _obter_arquivos_conta(phone)
    try:
        with open(estado_arq, "w", encoding="utf-8") as f:
            json.dump({"data": str(date.today()), "entradas": n}, f)
    except Exception:
        pass


def padronizar_url(url):
    if not url:
        return ""
    u = url.strip().split(" #")[0].rstrip("/")
    u = re.sub(r"^https?://", "", u, flags=re.I)
    return u.lower()


def carregar_todos_feitos(phone=None):
    feitos = set()
    feitos_arq, _ = _obter_arquivos_conta(phone)
    if os.path.exists(feitos_arq):
        try:
            with open(feitos_arq, encoding="utf-8", errors="ignore") as f:
                for l in f:
                    norm = padronizar_url(l)
                    if norm:
                        feitos.add(norm)
        except Exception:
            pass
    return feitos


def ja_entrou(url, phone=None):
    norm = padronizar_url(url)
    if not norm:
        return False
    return norm in carregar_todos_feitos(phone)


def marcar_feito(url, phone=None):
    u = url.strip()
    if not u:
        return
    feitos_arq, _ = _obter_arquivos_conta(phone)
    try:
        with open(feitos_arq, "a", encoding="utf-8") as f:
            f.write(u + "\n")
    except Exception:
        pass


def tentar_auto_conectar():
    global _client
    if cliente_conectado():
        return True
    conf = carregar_conf()
    api_id = conf.get("api_id")
    api_hash = conf.get("api_hash")
    if not api_id or not api_hash:
        return False

    # 1. Tentar telefone configurado se tiver arquivo de sessão
    phone = conf.get("phone")
    if phone:
        sess_path = _obter_sessao_path(phone)
        if os.path.exists(sess_path + ".session"):
            try:
                garantir_loop()
                async def _auto_phone():
                    c = await _criar_cliente(api_id, api_hash, phone)
                    return await c.is_user_authorized()
                if run_async(_auto_phone()):
                    return True
            except Exception as e:
                print(f"[i] Auto-conexao telefone {phone}: {e}")

    # 2. Fallback para qualquer sessão autorizada disponível (ex: Nayara)
    import glob
    for sf in glob.glob(os.path.join(DADOS, "sessao_*.session")):
        base = os.path.basename(sf).replace(".session", "")
        if base in ("sessao_segura", "sessao_dashboard") or base.startswith("sessao_rt_"):
            continue
        try:
            garantir_loop()
            async def _auto_other(b=base):
                global _client
                c = TelegramClient(os.path.join(DADOS, b), int(api_id), api_hash, loop=_loop)
                await c.connect()
                if await c.is_user_authorized():
                    _client = c
                    me = await c.get_me()
                    if me and me.phone:
                        conf["phone"] = f"+{me.phone}"
                        salvar_conf(conf)
                    return True
                await c.disconnect()
                return False
            if run_async(_auto_other()):
                return True
        except Exception:
            pass

    return False


# ══════════════════════════════════════════════════════════════════════════════
# Rotas Flask — páginas
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    conf = carregar_conf()
    return render_template("index.html", tem_conf=bool(conf.get("api_id")))


# ══════════════════════════════════════════════════════════════════════════════
# Detecção Rigorosa de Bot Anti-Spam / Rose / Captcha
# ══════════════════════════════════════════════════════════════════════════════

TERMOS_ANTISPAM_BLOQUEIO = [
    'rose', 'missrose', '@missrose_bot', 'captcha', 'capchut', 'recaptcha',
    'grouphelp', 'group help', '@grouphelp_bot', 'shieldy', 'shield', '@shieldy_bot',
    'guard', 'guardian', '@guardian_support_bot', 'antispam', 'anti-spam', 'anti_spam',
    'bot de segurança', 'bot de seguranca', 'bot de proteção', 'bot de protecao',
    'proteger', 'protegido', 'press the button', 'unmute', 'desmutar',
    'clique no botão', 'clique no botao', 'aperte o botão', 'aperte o botao',
    'resolva', 'teste humano', 'apenas membros verificados', 'safeguard', 'protectron',
    'verificação', 'verificacao', 'verifique', 'verificar-se'
]

def eh_grupo_com_antispam(g):
    """Verifica se o grupo possui qualquer menção a bot de anti-spam, Rose, Captcha ou verificação humana."""
    if g.get("requer_captcha") is True:
        return True
    tipo = str(g.get("tipo", "")).lower()
    if "anti" in tipo or "spam" in tipo or "captcha" in tipo:
        return True
    texto = f"{g.get('titulo', '')} {g.get('desc', '')} {g.get('url', '')}".lower()
    return any(t in texto for t in TERMOS_ANTISPAM_BLOQUEIO)


# ══════════════════════════════════════════════════════════════════════════════
# Rotas Flask — API
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/config", methods=["GET"])
def api_get_config():
    conf = carregar_conf()
    return jsonify({
        "api_id":  conf.get("api_id", ""),
        "api_hash": conf.get("api_hash", ""),
        "phone":   conf.get("phone", ""),
        "conectado": cliente_conectado(),
    })


@app.route("/api/config", methods=["POST"])
def api_salvar_config():
    d = request.json or {}
    api_id  = str(d.get("api_id", "")).strip()
    api_hash = d.get("api_hash", "").strip()
    phone   = d.get("phone", "").strip()
    if not api_id or not api_hash or not phone:
        return jsonify({"ok": False, "erro": "Preencha todos os campos"}), 400
    salvar_conf({"api_id": api_id, "api_hash": api_hash, "phone": phone})
    return jsonify({"ok": True})


@app.route("/api/conectar", methods=["POST"])
def api_conectar():
    global _phone_code_hash
    conf = carregar_conf()
    if not conf.get("api_id"):
        return jsonify({"ok": False, "erro": "Configure a API primeiro"}), 400

    phone = conf.get("phone", "").strip()
    if not phone:
        return jsonify({"ok": False, "erro": "Informe o número de telefone"}), 400

    garantir_loop()
    try:
        async def _conectar():
            global _phone_code_hash
            c = await _criar_cliente(conf["api_id"], conf["api_hash"], phone)
            
            # Se a sessão deste telefone específico já estiver autorizada
            if await c.is_user_authorized():
                me = await c.get_me()
                clean_target = re.sub(r"[^0-9]", "", phone)
                me_clean = re.sub(r"[^0-9]", "", me.phone or "")
                if clean_target and me_clean and clean_target == me_clean:
                    return {"ok": True, "ja_logado": True, "usuario": me.first_name}

            # Envia código para o novo telefone
            result = await c.send_code_request(phone)
            _phone_code_hash = result.phone_code_hash
            return {"ok": True, "ja_logado": False}

        res = run_async(_conectar())
        return jsonify(res)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/verificar_codigo", methods=["POST"])
def api_verificar_codigo():
    global _phone_code_hash
    d = request.json or {}
    codigo = str(d.get("codigo", "")).strip()
    senha2fa = d.get("senha2fa", "").strip()
    conf = carregar_conf()

    if not codigo:
        return jsonify({"ok": False, "erro": "Informe o código"}), 400

    try:
        async def _verificar():
            global _phone_code_hash
            try:
                await _client.sign_in(conf["phone"], codigo,
                                      phone_code_hash=_phone_code_hash)
            except SessionPasswordNeededError:
                if not senha2fa:
                    return {"ok": False, "precisa_2fa": True}
                await _client.sign_in(password=senha2fa)

            # Atualizar sessão padrão e manter cópia
            clean_phone = re.sub(r"[^0-9]", "", conf.get("phone", ""))
            sess_orig = os.path.join(DADOS, f"sessao_{clean_phone}.session")
            sess_dash = os.path.join(DADOS, "sessao_dashboard.session")
            if os.path.exists(sess_orig):
                try:
                    import shutil
                    shutil.copy2(sess_orig, sess_dash)
                except Exception:
                    pass

            me = await _client.get_me()
            nome_completo = (me.first_name + (" " + (me.last_name or "")).strip()) if me else "Telegram"
            partes = nome_completo.split()
            iniciais = (partes[0][0] + (partes[-1][0] if len(partes) > 1 else "")).upper() if partes else "TG"

            # Atualizar metadados salvos
            meta_path = os.path.join(DADOS, "contas_meta.json")
            meta = {}
            if os.path.exists(meta_path):
                try:
                    with open(meta_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                except Exception:
                    pass
            fmt_tel = f"+{clean_phone}"
            meta[fmt_tel] = {
                "nome": nome_completo,
                "iniciais": iniciais,
                "telefone": fmt_tel,
                "premium": bool(getattr(me, "premium", False)),
                "gradient": "linear-gradient(135deg, #3b82f6, #6366f1)"
            }
            try:
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2, ensure_ascii=False)
            except Exception:
                pass

            return {"ok": True, "usuario": nome_completo}

        res = run_async(_verificar())
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/contas", methods=["GET"])
def api_listar_contas():
    """Lista todas as contas Telegram salvas no sistema com metadados visuais (nome, iniciais, premium, ativa)."""
    contas = []
    meta_path = os.path.join(DADOS, "contas_meta.json")
    meta_dict = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta_dict = json.load(f)
        except Exception:
            pass

    import glob
    sess_files = glob.glob(os.path.join(DADOS, "sessao_*.session"))
    telefone_atual = ""
    with _client_lock:
        if cliente_conectado():
            try:
                async def _me_atual():
                    if await _client.is_user_authorized():
                        me = await _client.get_me()
                        return me.phone if me else ""
                    return ""
                telefone_atual = run_async(_me_atual()) or ""
            except Exception:
                pass

    # Garantir que as contas do contas_meta também apareçam
    fones_vistos = set()

    for sf in sess_files:
        basename = os.path.basename(sf).replace(".session", "")
        if basename in ("sessao_dashboard", "sessao_segura") or basename.startswith("sessao_rt_"):
            continue
        phone_num = basename.replace("sessao_", "")
        if not phone_num.isdigit() or len(phone_num) < 8:
            continue
        fmt_tel = f"+{phone_num}"
        fones_vistos.add(fmt_tel)
        is_active = (re.sub(r"[^0-9]", "", telefone_atual) == phone_num)

        acc_info = meta_dict.get(fmt_tel, {})
        nome = acc_info.get("nome") or (f"Conta {phone_num[-4:]}")
        if is_active and _client:
            try:
                async def _me_name():
                    me = await _client.get_me()
                    return me.first_name + (" " + (me.last_name or "")).strip() if me else ""
                n = run_async(_me_name())
                if n:
                    nome = n
            except Exception:
                pass

        partes_nome = nome.split()
        iniciais = (partes_nome[0][0] + (partes_nome[-1][0] if len(partes_nome) > 1 else "")).upper()

        contas.append({
            "telefone": fmt_tel,
            "arquivo": basename,
            "nome": nome,
            "iniciais": iniciais,
            "premium": bool(acc_info.get("premium", False)),
            "gradient": acc_info.get("gradient", "linear-gradient(135deg, #3b82f6, #6366f1)"),
            "foto": acc_info.get("foto", ""),
            "ativa": is_active
        })

    for fmt_tel, info in meta_dict.items():
        if fmt_tel not in fones_vistos:
            is_active = (re.sub(r"[^0-9]", "", telefone_atual) == re.sub(r"[^0-9]", "", fmt_tel))
            contas.append({
                "telefone": fmt_tel,
                "arquivo": f"sessao_{re.sub(r'[^0-9]', '', fmt_tel)}",
                "nome": info.get("nome", "Conta"),
                "iniciais": info.get("iniciais", "TG"),
                "premium": bool(info.get("premium", False)),
                "gradient": info.get("gradient", "linear-gradient(135deg, #3b82f6, #6366f1)"),
                "foto": info.get("foto", ""),
                "ativa": is_active
            })

    contas.sort(key=lambda x: not x["ativa"])
    return jsonify({"contas": contas, "atual": telefone_atual})


@app.route("/api/remover_conta", methods=["POST"])
def api_remover_conta():
    d = request.json or {}
    telefone = str(d.get("telefone", "")).strip()
    clean = re.sub(r"[^0-9]", "", telefone)
    if not clean:
        return jsonify({"ok": False, "erro": "Telefone inválido"}), 400

    global _client
    sess_path = os.path.join(DADOS, f"sessao_{clean}.session")

    # Se o cliente ativo for esta conta, desconectar antes
    try:
        if _client and _client.is_connected():
            run_async(_client.disconnect())
            _client = None
    except Exception:
        pass

    if os.path.exists(sess_path):
        try:
            os.remove(sess_path)
        except Exception:
            try:
                trash_path = os.path.join(DADOS, f"sessao_{clean}_{int(time.time())}.trash")
                os.replace(sess_path, trash_path)
            except Exception:
                pass

    meta_path = os.path.join(DADOS, "contas_meta.json")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            if f"+{clean}" in meta:
                del meta[f"+{clean}"]
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

    return jsonify({"ok": True})

    return jsonify({"ok": True})


@app.route("/api/status")
def api_status():
    if not cliente_conectado():
        tentar_auto_conectar()
    if not cliente_conectado():
        return jsonify({"conectado": False, "usuario": None})
    try:
        async def _me():
            if not await _client.is_user_authorized():
                return {"conectado": False, "usuario": None}
            me = await _client.get_me()
            if not me:
                return {"conectado": False, "usuario": None}
            return {
                "conectado": True,
                "usuario": me.first_name + (" " + (me.last_name or "")).strip(),
                "username": "@" + me.username if me.username else "",
                "telefone": me.phone,
            }
        return jsonify(run_async(_me()))
    except Exception as e:
        return jsonify({"conectado": False, "erro": str(e)})


def trocar_para_telefone_sincrono(telefone):
    """Garante que _client está conectado ao telefone especificado usando pool em memória (0ms switch)."""
    global _client, _clients_pool
    if not telefone:
        return False
    clean = re.sub(r"[^0-9]", "", str(telefone))
    if not clean:
        return False
    conf = carregar_conf()
    api_id = conf.get("api_id")
    api_hash = conf.get("api_hash")
    if not api_id or not api_hash:
        return False

    with _client_lock:
        # 1. Se já está no pool de clientes conectados, alterna em memória sem desconectar!
        if clean in _clients_pool:
            c_pool = _clients_pool[clean]
            try:
                garantir_loop()
                async def _usar_pool():
                    global _client
                    if not c_pool.is_connected():
                        await c_pool.connect()
                    if await c_pool.is_user_authorized():
                        _client = c_pool
                        return True
                    return False
                if run_async(_usar_pool()):
                    conf["phone"] = f"+{clean}"
                    salvar_conf(conf)
                    return True
            except Exception:
                _clients_pool.pop(clean, None)

        sess_check = os.path.join(DADOS, f"sessao_{clean}.session")
        if not os.path.exists(sess_check):
            return False

        garantir_loop()
        async def _fazer_troca():
            global _client, _clients_pool
            c = await _criar_cliente(api_id, api_hash, clean)
            try:
                autorizado = await c.is_user_authorized()
            except Exception:
                autorizado = False
            if autorizado:
                _client = c
                _clients_pool[clean] = c
                conf["phone"] = f"+{clean}"
                salvar_conf(conf)
                return True
            _clients_pool.pop(clean, None)
            return False

        try:
            return run_async(_fazer_troca())
        except Exception as e:
            print(f"Erro ao trocar conta para {telefone}: {e}")
            return False


@app.route("/api/trocar_conta", methods=["POST"])
def api_trocar_conta():
    """Alterna para outra conta Telegram salva."""
    d = request.json or {}
    telefone = str(d.get("telefone", "")).strip()
    if not telefone:
        return jsonify({"ok": False, "erro": "Telefone não informado"}), 400

    conf = carregar_conf()
    api_id = conf.get("api_id")
    api_hash = conf.get("api_hash")
    if not api_id or not api_hash:
        return jsonify({"ok": False, "erro": "Configure a API primeiro"}), 400

    conf["phone"] = telefone
    salvar_conf(conf)

    garantir_loop()
    try:
        async def _trocar():
            global _client
            c = await _criar_cliente(api_id, api_hash, telefone)
            if await c.is_user_authorized():
                me = await c.get_me()
                clean_phone = re.sub(r"[^0-9]", "", telefone)
                sess_orig = os.path.join(DADOS, f"sessao_{clean_phone}.session")
                sess_dash = os.path.join(DADOS, "sessao_dashboard.session")
                if os.path.exists(sess_orig):
                    try:
                        import shutil
                        shutil.copy2(sess_orig, sess_dash)
                    except Exception:
                        pass
                return {
                    "ok": True,
                    "usuario": me.first_name + (" " + (me.last_name or "")).strip(),
                    "username": "@" + me.username if me.username else "",
                    "telefone": me.phone
                }
            return {
                "ok": False,
                "precisa_conectar": True,
                "telefone": telefone,
                "erro": "Esta conta ainda precisa do código de verificação para conectar pela primeira vez."
            }

        res = run_async(_trocar())
        return jsonify(res)
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/grupos")
def api_grupos():
    if not os.path.exists(GRUPOS_ARQ):
        return jsonify({"ok": False, "erro": "br_validados.json não encontrado"}), 404

    with open(GRUPOS_ARQ, encoding="utf-8") as f:
        grupos = json.load(f)

    feitos = carregar_todos_feitos()

    ja_entrou_count = 0
    for g in grupos:
        if g.get("foto") and not g.get("imagem"):
            g["imagem"] = g["foto"]
        entrou = padronizar_url(g["url"]) in feitos
        g["ja_entrou"] = entrou
        if entrou:
            ja_entrou_count += 1
        # Classificação estrita anti-spam / rose / captcha
        if eh_grupo_com_antispam(g):
            g["requer_captcha"] = True
            g["tipo"] = "antispam"
        else:
            g["requer_captcha"] = False
            g["tipo"] = "livre"

    # Ordenar priorizando Grupos de Bate-Papo & Amizade Primeiro!
    grupos.sort(key=lambda x: (-int(bool(x.get("is_chat_amizade"))), -(x.get("membros") or -1)))

    estado = carregar_estado()
    conf = carregar_conf()

    com_captcha = sum(1 for g in grupos if g.get("requer_captcha"))
    livres = len(grupos) - com_captcha

    return jsonify({
        "ok": True,
        "grupos": grupos,
        "total": len(grupos),
        "livres": livres,
        "com_captcha": com_captcha,
        "ja_entrou": ja_entrou_count,
        "entradas_hoje": estado,
        "limite_diario": int(conf.get("limite_diario", 20)),
        "conectado": cliente_conectado(),
    })


def remover_grupo_definitivo(url):
    """Remove o grupo permanentemente de br_validados.json para nunca mais aparecer e registra em descartados."""
    try:
        norm = padronizar_url(url)
        with open(GRUPOS_ARQ, "r", encoding="utf-8") as f:
            grupos = json.load(f)
        novos = [g for g in grupos if padronizar_url(g.get("url", "")) != norm]
        if len(novos) != len(grupos):
            with open(GRUPOS_ARQ, "w", encoding="utf-8") as f:
                json.dump(novos, f, ensure_ascii=False, indent=2)
            arq_desc = os.path.join(DADOS, "descartados_sem_chat.txt")
            with open(arq_desc, "a", encoding="utf-8") as f:
                f.write(f"{url}\n")
            try:
                import auto_sync_vercel
                threading.Thread(target=auto_sync_vercel.sync_all, daemon=True).start()
            except Exception as e:
                print(f"Erro auto sync vercel: {e}")
    except Exception as e:
        print(f"Erro ao remover grupo definitivo: {e}")


def reclassificar_grupo_antispam(url):
    """Reclassifica o grupo como requer_captcha=True e tipo='antispam' para ser excluído do Auto-Join.
       IMPORTANTE: Mantém o grupo no catálogo manual para quem desejar entrar manualmente."""
    try:
        norm = padronizar_url(url)
        for path in [GRUPOS_ARQ, os.path.join(BASE, "site_vercel", "grupos.json")]:
            if not os.path.exists(path):
                continue
            with open(path, "r", encoding="utf-8") as f:
                grupos = json.load(f)
            alterou = False
            for g in grupos:
                if padronizar_url(g.get("url", "")) == norm:
                    g["tipo"] = "antispam"
                    g["requer_captcha"] = True
                    alterou = True
                    break
            if alterou:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(grupos, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Erro ao reclassificar grupo como antispam: {e}")


async def detectar_contra_spam_ativo(res, client=None):
    """Detecta se o grupo possui verificação humana, captcha, Akira Baltflame, Group Help, Rose ou restrição contra spam."""
    c_use = client or _client
    chats = getattr(res, "chats", [])
    if not chats or not c_use:
        return None
    chat = chats[0]
    try:
        chat = await c_use.get_entity(chat)
    except Exception:
        pass

    try:
        # 1. Checar se usuário foi restringido ou mutado
        perms = await c_use.get_permissions(chat, "me")
        if getattr(perms, "is_banned", False):
            return "Usuário restrito/mutado por bot do grupo"
        part = getattr(perms, "participant", None)
        banned_p = getattr(part, "banned_rights", None)
        if banned_p and getattr(banned_p, "send_messages", False):
            return "Envio de mensagens bloqueado por bot/regras"

        # 2. Checar últimas mensagens procurando botões de captcha / contra-spam
        async for m in c_use.iter_messages(chat, limit=8):
            txt = (m.text or "").lower()
            if m.buttons:
                for row in m.buttons:
                    for b in row:
                        btxt = (b.text or "").lower()
                        if any(k in btxt for k in ["desbloqueie", "desbloquear", "verificar", "humano", "regras", "estou aqui", "unmute", "clique aqui", "confirmar", "segurança", "seguranca"]):
                            return f"Botão de captcha: '{b.text}'"

            termos_alerta = [
                "pressione o botão", "pressione o botao", "para poder escrever",
                "desbloqueie-me", "desbloqueie", "contra spam", "contra-spam",
                "anti-spam", "antispam", "group help", "grouphelp",
                "shieldy", "missrose", "rose bot", "verificação humana", "verificacao humana"
            ]
            if any(t in txt for t in termos_alerta):
                return f"Bot contra-spam: {(m.text or '')[:40]}"
    except Exception as e:
        logger.debug(f"Erro ao detectar contra-spam: {e}")
    return None


async def checar_e_sair_se_sem_chat(res, url, client=None, phone=None, modo="manual"):
    """Verifica se o chat e canal broadcast, proibe membros normais de enviar mensagem ou cobra estrelas.
       Se nao permitir enviar mensagem, sai do chat e remove do catalogo permanentemente."""
    c_use = client or _client
    chats = getattr(res, "chats", [])
    if not chats or not c_use:
        return None
    chat = chats[0]

    # Obter entidade completa com permissoes atualizadas
    try:
        chat = await c_use.get_entity(chat)
    except Exception:
        pass
    
    # 1. Canal de transmissao puro
    if getattr(chat, "broadcast", False):
        from telethon.tl.functions.channels import LeaveChannelRequest
        try:
            await c_use(LeaveChannelRequest(chat))
        except Exception:
            pass
        remover_grupo_definitivo(url)
        marcar_feito(url, phone=phone)
        return "Canal de transmissão / avisos (não é grupo de bate-papo)"

    # 2. default_banned_rights (Regra padrao: membros proibidos de mandar mensagem no grupo)
    banned = getattr(chat, "default_banned_rights", None)
    if banned and getattr(banned, "send_messages", False):
        from telethon.tl.functions.channels import LeaveChannelRequest
        try:
            await c_use(LeaveChannelRequest(chat))
        except Exception:
            pass
        remover_grupo_definitivo(url)
        marcar_feito(url, phone=phone)
        return "Grupo fechado (não é permitido enviar mensagens neste grupo)"

    # 3. Grupo pago (Telegram Stars por mensagem)
    try:
        from telethon.tl.functions.channels import GetFullChannelRequest
        full = await c_use(GetFullChannelRequest(chat))
        cf = getattr(full, "full_chat", None)
        if cf:
            paid_stars = getattr(cf, "send_paid_messages_stars", 0) or 0
            if paid_stars > 0:
                from telethon.tl.functions.channels import LeaveChannelRequest
                try:
                    await c_use(LeaveChannelRequest(chat))
                except Exception:
                    pass
                remover_grupo_definitivo(url)
                marcar_feito(url, phone=phone)
                return f"Grupo pago (cobra {paid_stars} Estrelas do Telegram por mensagem)"
    except Exception:
        pass

    return None


@app.route("/api/marcar", methods=["POST"])
def api_marcar():
    d = request.json or {}
    url = d.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "erro": "URL inválida"}), 400
    marcar_feito(url)
    return jsonify({"ok": True, "msg": "Grupo marcado como já entrado e removido da lista"})


@app.route("/api/descartar", methods=["POST"])
def api_descartar():
    d = request.json or {}
    url = d.get("url", "").strip()
    if not url:
        return jsonify({"ok": False, "erro": "URL inválida"}), 400
    remover_grupo_definitivo(url)
    marcar_feito(url)
    return jsonify({"ok": True, "msg": "Grupo descartado e removido do catálogo com sucesso!"})


@app.route("/api/sync_vercel", methods=["POST", "GET"])
def api_sync_vercel():
    try:
        import auto_sync_vercel
        threading.Thread(target=auto_sync_vercel.sync_all, daemon=True).start()
        return jsonify({"ok": True, "msg": "Sincronização com o GitHub e Vercel iniciada em segundo plano!"})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/importar_e_filtrar", methods=["POST", "OPTIONS"])
def api_importar_e_filtrar():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})
    
    texto = ""
    # 1. Se veio arquivo enviado via upload
    if request.files:
        for k in request.files:
            arq = request.files[k]
            if arq and getattr(arq, "filename", ""):
                try:
                    conteudo = arq.read().decode("utf-8", errors="ignore")
                    if conteudo:
                        texto += "\n" + conteudo
                except Exception:
                    pass
    
    # 2. Se veio JSON
    d = request.get_json(silent=True)
    if d and isinstance(d, dict):
        t = d.get("texto", "") or d.get("links", "") or d.get("lista", "")
        if t:
            texto += "\n" + str(t)
    
    # 3. Se veio form-data com texto
    if request.form:
        t = request.form.get("texto", "") or request.form.get("links", "") or request.form.get("lista", "")
        if t:
            texto += "\n" + str(t)

    texto = (texto or "").strip()
    if not texto:
        return jsonify({"sucesso": False, "erro": "Nenhum link ou arquivo fornecido."}), 400
    try:
        from moderador_engine import processar_e_adicionar_links
        res = processar_e_adicionar_links(texto)
        return jsonify(res)
    except Exception as e:
        return jsonify({"sucesso": False, "erro": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# Garimpar links do Telegram a partir de sites externos  (modo job assíncrono)
# ══════════════════════════════════════════════════════════════════════════════
import uuid as _uuid

_garimpo_jobs: dict = {}   # job_id -> {"status": ..., "progresso": {}, "result": None}


def _executar_garimpo_bg(job_id: str, urls_validas: list, subpages: bool, profundo: bool):
    """Roda em thread daemon para não bloquear o Flask."""
    try:
        _garimpo_jobs[job_id]["status"] = "running"
        _garimpo_jobs[job_id]["progresso"] = {
            "paginas": 0,
            "links_encontrados": 0,
            "mensagem": "Iniciando motor de garimpo profundo..."
        }

        def on_prog(d):
            _garimpo_jobs[job_id]["progresso"] = d

        from garimpador_engine import garimpar_site_completo
        todos_links_tg = garimpar_site_completo(
            urls_validas,
            subpages=subpages,
            profundo=profundo,
            progress_callback=on_prog
        )
        print(f"[GARIMPO BG] Links extraídos ({len(todos_links_tg)}): {todos_links_tg}")

        if not todos_links_tg:
            _garimpo_jobs[job_id].update({
                "status": "done",
                "result": {
                    "sucesso": True,
                    "sites_vasculhados": len(urls_validas),
                    "total_recebidos": 0,
                    "novos_analisados": 0,
                    "novos_aprovados": 0,
                    "descartados_expirados": 0,
                    "descartados_gringos": 0,
                    "descartados_sem_chat": 0,
                    "duplicados": 0,
                    "total_atual_site": 0,
                    "aviso": "Nenhum link do Telegram encontrado nos sites fornecidos."
                }
            })
            return

        _garimpo_jobs[job_id]["progresso"] = {
            "paginas": _garimpo_jobs[job_id]["progresso"].get("paginas", 0),
            "links_encontrados": len(todos_links_tg),
            "mensagem": f"Filtrando {len(todos_links_tg)} grupos coletados (Regras: Ativos + BR + Chat Aberto)..."
        }

        texto = "\n".join(sorted(todos_links_tg))
        from moderador_engine import processar_e_adicionar_links
        res = processar_e_adicionar_links(texto)
        res["sites_vasculhados"] = _garimpo_jobs[job_id]["progresso"].get("paginas", len(urls_validas))
        _garimpo_jobs[job_id].update({"status": "done", "result": res})

    except Exception as exc:
        import traceback
        _garimpo_jobs[job_id].update({
            "status": "error",
            "result": {"sucesso": False, "erro": str(exc), "trace": traceback.format_exc()}
        })


@app.route("/api/garimpar_site", methods=["POST", "OPTIONS"])
def api_garimpar_site():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    d = request.get_json(silent=True) or {}
    urls_input = d.get("urls", [])
    subpages   = bool(d.get("subpages", True))
    profundo   = bool(d.get("profundo", False))

    if isinstance(urls_input, str):
        urls_input = [u.strip() for u in urls_input.splitlines() if u.strip()]

    urls_validas = [u for u in urls_input if isinstance(u, str) and u.strip().startswith("http")]
    if not urls_validas:
        return jsonify({"sucesso": False, "erro": "Nenhuma URL válida fornecida."}), 400

    job_id = str(_uuid.uuid4())
    _garimpo_jobs[job_id] = {"status": "pending", "progresso": {}, "result": None}

    t = threading.Thread(
        target=_executar_garimpo_bg,
        args=(job_id, urls_validas, subpages, profundo),
        daemon=True
    )
    t.start()

    return jsonify({"sucesso": True, "job_id": job_id, "mensagem": "Garimpo iniciado."})


@app.route("/api/garimpar_site/status/<job_id>", methods=["GET"])
def api_garimpar_status(job_id):
    job = _garimpo_jobs.get(job_id)
    if not job:
        return jsonify({"sucesso": False, "erro": "Job não encontrado ou já coletado."}), 404

    resp = {
        "sucesso": True,
        "status":    job["status"],   # "pending" | "running" | "done" | "error"
        "progresso": job.get("progresso", {}),
    }
    if job["status"] in ("done", "error"):
        resp["result"] = job["result"]
        _garimpo_jobs.pop(job_id, None)   # libera memória

    return jsonify(resp)


# ══════════════════════════════════════════════════════════════════════════════
# Turbo Garimpo 5K (Combot + Telethon In-App + Diretórios Web)
# ══════════════════════════════════════════════════════════════════════════════

def _executar_turbo_5k_bg(job_id: str):
    """Roda em background para turbinar a base de grupos até 5.000+."""
    try:
        from garimpador_engine import clean_tg_link
        _garimpo_jobs[job_id]["status"] = "running"
        _garimpo_jobs[job_id]["progresso"] = {"mensagem": "Iniciando Turbo Garimpo 5K...", "links_encontrados": 0}

        todos_links = set()

        def notificar(msg):
            _garimpo_jobs[job_id]["progresso"] = {
                "mensagem": msg,
                "links_encontrados": len(todos_links)
            }

        # ─── ETAPA 1: COMBOT TOP GROUPS BRASIL / PORTUGAL ───────────────
        notificar("Etapa 1/4: Coletando ranking Combot Brasil (100% ativos)...")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        for p in range(1, 10):
            try:
                r = requests.get(f"https://combot.org/top/telegram/groups/pt?page={p}", headers=headers, timeout=10)
                if r.status_code == 200:
                    found = re.findall(r'https?://(?:t|telegram)\.me/[a-zA-Z0-9_+\-]+', r.text)
                    for u in found:
                        c = clean_tg_link(u)
                        if c:
                            todos_links.add(c)
                notificar(f"Combot: Página {p} processada ({len(todos_links)} links até agora)...")
            except Exception:
                pass

        # ─── ETAPA 2: TELETHON IN-APP (Mega Canais de Divulgação BR) ─────
        if cliente_conectado():
            notificar("Etapa 2/4: Varrendo canais de divulgação do Telegram com o Telethon...")
            CANAIS_DIVULGACAO = [
                "divulgueaqui", "divulgueaquioficial", "linksdegruposbrasil", "centraldelinksbr",
                "gruposbrasiloficial", "divulgacaotgbrasil", "conectarlinks", "gruposbrasill",
                "linksbrasil", "divulga_grupos_br", "grupotelegrambrasil", "amizadesbrasil",
                "canaisegruposbrasil", "divulgacoesbrasil", "divulgueaqui_br", "acheigrupos",
                "grupostelegrambr", "divulgacaobr", "conexoesbrasil", "linksdegrupos",
                "divulgueaquisp", "gruposbrlinks", "hubgruposbr", "divulgaaquigrupos"
            ]

            async def _varrer_canais():
                sub_links = set()
                for c_nome in CANAIS_DIVULGACAO:
                    try:
                        entity = await _client.get_entity(c_nome)
                        async for msg in _client.iter_messages(entity, limit=250):
                            texto_msg = msg.text or ""
                            for m in re.findall(r'https?://(?:t|telegram)\.me/[a-zA-Z0-9_+\-]+', texto_msg):
                                c = clean_tg_link(m)
                                if c:
                                    sub_links.add(c)
                    except Exception:
                        pass
                return sub_links

            try:
                links_tg_inapp = run_async(_varrer_canais())
                todos_links.update(links_tg_inapp)
                notificar(f"Telethon: {len(links_tg_inapp)} links frescos coletados dos canais de divulgação!")
            except Exception as e:
                print(f"Aviso no Telethon in-app: {e}")

        # ─── ETAPA 3: TOPGRUPOSBR + DIVULGRUPO EXPANDIDO ─────────────────
        notificar("Etapa 3/4: Coletando diretórios web adicionais...")
        try:
            r = requests.get('https://topgruposbr.com/assets/index-B6gS3jMR.js', headers=headers, timeout=10)
            key_m = re.search(r'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9\.[a-zA-Z0-9_\-]+\.[a-zA-Z0-9_\-]+', r.text)
            if key_m:
                key = key_m.group(0)
                for offset in (0, 1000, 2000):
                    h_supa = {'apikey': key, 'Authorization': f'Bearer {key}', 'Range': f'{offset}-{offset + 999}'}
                    rq = requests.get('https://kbubfcenmmydvxyqkayf.supabase.co/rest/v1/telegram_groups?select=telegram_url&is_active=eq.true', headers=h_supa, timeout=10)
                    if rq.status_code in (200, 206):
                        for row in rq.json():
                            u = clean_tg_link(row.get('telegram_url', ''))
                            if u:
                                todos_links.add(u)
        except Exception:
            pass

        try:
            from super_garimpo_geral import coletar_divulgrupo
            todos_links.update(coletar_divulgrupo())
        except Exception:
            pass

        notificar(f"Total bruto: {len(todos_links)} links coletados! Iniciando filtro estrito...")

        # ─── ETAPA 4: FILTRO ESTRITO & ADIÇÃO AO SITE ───────────────────
        notificar(f"Etapa 4/4: Filtrando {len(todos_links)} grupos (Ativos + 100% BR + Chat Aberto)...")
        texto_todos = "\n".join(sorted(todos_links))
        from moderador_engine import processar_e_adicionar_links
        res = processar_e_adicionar_links(texto_todos)

        _garimpo_jobs[job_id].update({"status": "done", "result": res})

    except Exception as exc:
        import traceback
        _garimpo_jobs[job_id].update({
            "status": "error",
            "result": {"sucesso": False, "erro": str(exc), "trace": traceback.format_exc()}
        })


@app.route("/api/turbo_garimpo_5k", methods=["POST", "OPTIONS"])
def api_turbo_garimpo_5k():
    if request.method == "OPTIONS":
        return jsonify({"ok": True})

    job_id = str(_uuid.uuid4())
    _garimpo_jobs[job_id] = {
        "status": "pending",
        "progresso": {"mensagem": "Iniciando Turbo Garimpo 5K...", "links_encontrados": 0},
        "result": None
    }

    t = threading.Thread(
        target=_executar_turbo_5k_bg,
        args=(job_id,),
        daemon=True
    )
    t.start()

    return jsonify({"sucesso": True, "job_id": job_id, "mensagem": "Turbo Garimpo 5K iniciado."})


_flood_ate = {}  # telefone -> timestamp até quando o Telegram bloqueou entradas


def _fmt_espera(seg):
    seg = int(seg)
    h, r = divmod(seg, 3600)
    m = r // 60
    return f"{h}h{m:02d}min" if h else f"{m}min"


def rotacionar_para_proxima_conta_valida(ignorar_telefones=None):
    """Procura outra conta autorizada salva em dados/sessao_*.session que NÃO esteja em FloodWait.
    Se encontrar, conecta na nova usando o pool, atualiza config.json e retorna dict com info."""
    with _client_lock:
        return _rotacionar_para_proxima_conta_valida_impl(ignorar_telefones)

def _rotacionar_para_proxima_conta_valida_impl(ignorar_telefones=None):
    global _client
    if ignorar_telefones is None:
        ignorar_telefones = set()
    else:
        ignorar_telefones = {re.sub(r"[^0-9]", "", str(t)) for t in ignorar_telefones if t}

    conf = carregar_conf()
    api_id = conf.get("api_id")
    api_hash = conf.get("api_hash")
    if not api_id or not api_hash:
        return None

    import glob
    sess_files = glob.glob(os.path.join(DADOS, "sessao_*.session"))
    agora = time.time()

    for sf in sess_files:
        base = os.path.basename(sf).replace(".session", "")
        if base in ("sessao_dashboard", "sessao_segura") or base.startswith("sessao_rt_"):
            continue
        clean_num = base.replace("sessao_", "")
        fmt_tel = f"+{clean_num}"

        # Ignorar se estiver no set de ignorados
        if clean_num in ignorar_telefones:
            continue

        # Ignorar se estiver em FloodWait ativo
        if _flood_ate.get(fmt_tel, 0) > agora or _flood_ate.get(clean_num, 0) > agora:
            continue

        # Tentar conectar nesta conta
        try:
            garantir_loop()
            async def _conectar_nova(cn=clean_num, ft=fmt_tel):
                global _client
                c = await _criar_cliente(api_id, api_hash, cn)
                if await c.is_user_authorized():
                    _client = c
                    me = await c.get_me()
                    nome_conta = me.first_name + (" " + (me.last_name or "")).strip() if me else ft
                    return {"telefone": ft, "nome": nome_conta, "usuario": nome_conta}
                return None

            nova = run_async(_conectar_nova())
            if nova:
                conf["phone"] = nova["telefone"]
                salvar_conf(conf)
                try:
                    import shutil
                    shutil.copy2(sf, os.path.join(DADOS, "sessao_dashboard.session"))
                except Exception:
                    pass
                return nova
        except Exception:
            continue

    return None


@app.route("/api/entrar", methods=["POST"])
def api_entrar():
    d = request.json or {}
    url = d.get("url", "").strip()
    tel_solicitado = str(d.get("telefone", "")).strip()
    modo = str(d.get("modo", "")).lower()

    # No Modo Automático, não entra em grupos identificados com Contra Spam / Captcha!
    if modo == "auto" and url:
        try:
            with open(GRUPOS_ARQ, "r", encoding="utf-8") as f_g:
                catalogo = json.load(f_g)
            norm = padronizar_url(url)
            for g_item in catalogo:
                if padronizar_url(g_item.get("url", "")) == norm:
                    if g_item.get("requer_captcha") or g_item.get("tipo") == "antispam" or eh_grupo_com_antispam(g_item):
                        return jsonify({
                            "ok": False,
                            "status": "contra_spam",
                            "erro": "Grupo possui proteção Contra Spam / Captcha! Ignorado no Modo Automático (mantido no Modo Manual)."
                        })
        except Exception:
            pass

    # 1. Resolver o cliente específico desta aba/conta (com lock rápido apenas para pegar do pool)
    conta_alternada_info = None
    conta_ativa_real = None
    c_conta = None
    clean_conta = re.sub(r"[^0-9]", "", tel_solicitado) if tel_solicitado else ""

    with _client_lock:
        if tel_solicitado:
            if trocar_para_telefone_sincrono(tel_solicitado):
                c_conta = _clients_pool.get(clean_conta) or _client
            else:
                nova_fb = rotacionar_para_proxima_conta_valida(ignorar_telefones={tel_solicitado})
                if nova_fb:
                    conta_alternada_info = nova_fb["nome"]
                    conta_ativa_real = {"phone": nova_fb["telefone"], "nome": nova_fb["nome"]}
                    clean_conta = re.sub(r"[^0-9]", "", nova_fb["telefone"])
                    c_conta = _clients_pool.get(clean_conta) or _client
                else:
                    tentar_auto_conectar()
                    c_conta = _client
                    clean_conta = _obter_telefone_ativo()
        else:
            if not cliente_conectado():
                tentar_auto_conectar()
            if not cliente_conectado():
                nova_fb2 = rotacionar_para_proxima_conta_valida()
                if nova_fb2:
                    conta_alternada_info = nova_fb2["nome"]
                    conta_ativa_real = {"phone": nova_fb2["telefone"], "nome": nova_fb2["nome"]}
                    clean_conta = re.sub(r"[^0-9]", "", nova_fb2["telefone"])
            c_conta = _clients_pool.get(clean_conta) or _client
            if not clean_conta:
                clean_conta = _obter_telefone_ativo()

    if c_conta is None or not c_conta.is_connected():
        return jsonify({"ok": False, "status": "sem_conexao", "reutilizar": True, "erro": "Não conectado ao Telegram"}), 200

    # 2. Executar a entrada usando o lock EXCLUSIVO desta conta (permite 6+ abas em paralelo!)
    lock_desta_conta = obter_lock_conta(clean_conta)
    with lock_desta_conta:
        _fmt_f = f"+{clean_conta}" if clean_conta else ""
        _resta = max(_flood_ate.get(_fmt_f, 0), _flood_ate.get(clean_conta, 0)) - time.time()

        if _resta > 0:
            nova = None if (modo == "auto" and tel_solicitado) else rotacionar_para_proxima_conta_valida(ignorar_telefones={_fmt_f, clean_conta})
            if nova:
                conta_alternada_info = nova["nome"]
                conta_ativa_real = {"phone": nova["telefone"], "nome": nova["nome"]}
                clean_conta = re.sub(r"[^0-9]", "", nova["telefone"])
                c_conta = _clients_pool.get(clean_conta) or _client
            else:
                return jsonify({
                    "ok": False,
                    "status": "flood",
                    "reutilizar": True,
                    "espera_segundos": int(_resta) + 5,
                    "erro": f"Pausa de segurança do próprio Telegram nesta conta ({_fmt_espera(_resta)}). O grupo foi mantido na fila!"
                }), 429

        if not url:
            return jsonify({"ok": False, "erro": "URL inválida"}), 400

        if ja_entrou(url, phone=clean_conta):
            return jsonify({"ok": True, "status": "ja_membro", "msg": "Já está neste grupo", "conta_ativa_real": conta_ativa_real})

        tipo, valor = normalizar_link(url)
        if not tipo:
            return jsonify({"ok": False, "erro": "Link não reconhecido"}), 400

        try:
            async def _entrar(cli=c_conta, ph=clean_conta):
                if tipo == "invite":
                    res = await cli(ImportChatInviteRequest(valor))
                else:
                    res = await cli(JoinChannelRequest(valor))

                # Checar se por acaso era canal broadcast OU se não permite enviar mensagens
                motivo_invalido = await checar_e_sair_se_sem_chat(res, url, client=cli, phone=ph, modo=modo)
                if motivo_invalido:
                    return {
                        "ok": False,
                        "status": "sem_chat",
                        "erro": f"Descartado ({motivo_invalido})! A conta saiu automaticamente e o grupo foi removido da lista permanentemente."
                    }

                # Se for Modo Automático, checar se ativou bot contra-spam / captcha
                if modo == "auto":
                    await asyncio.sleep(1.2)
                    motivo_spam = await detectar_contra_spam_ativo(res, client=cli)
                    if motivo_spam:
                        chats = getattr(res, "chats", [])
                        if chats:
                            try:
                                await cli(LeaveChannelRequest(chats[0]))
                            except Exception:
                                pass
                        reclassificar_grupo_antispam(url)
                        return {
                            "ok": False,
                            "status": "contra_spam",
                            "erro": f"Proteção Contra Spam detectada ({motivo_spam})! A conta saiu automaticamente. O grupo continua disponível no Modo Manual."
                        }

                marcar_feito(url, phone=ph)
                estado = carregar_estado(phone=ph)
                novo_estado = estado + 1
                salvar_estado(novo_estado, phone=ph)
                msg_ok = f"Entrou com sucesso! (Alternado para {conta_alternada_info})" if conta_alternada_info else "Sucesso! Ingressado no grupo."
                return {"ok": True, "status": "entrou", "msg": msg_ok, "conta_alternada": conta_alternada_info, "conta_ativa_real": conta_ativa_real, "entradas_hoje": novo_estado}

            return jsonify(run_async(_entrar()))

        except UserAlreadyParticipantError:
            async def _checar_ja_membro(cli=c_conta, ph=clean_conta):
                try:
                    chat = await cli.get_entity(valor)
                    class DummyRes:
                        chats = [chat]
                    motivo = await checar_e_sair_se_sem_chat(DummyRes(), url, client=cli, phone=ph, modo=modo)
                    if motivo:
                        return {"ok": False, "status": "sem_chat", "erro": f"Descartado ({motivo})! A conta saiu automaticamente e o grupo foi removido da lista permanentemente."}
                except Exception:
                    pass
                marcar_feito(url, phone=ph)
                return {"ok": True, "status": "ja_membro", "msg": "Já era membro", "conta_ativa_real": conta_ativa_real}
            return jsonify(run_async(_checar_ja_membro()))
        except InviteHashExpiredError:
            remover_grupo_definitivo(url)
            marcar_feito(url, phone=clean_conta)
            return jsonify({"ok": False, "status": "expirado", "erro": "Link expirado (removido do catálogo automaticamente!)"})
        except InviteRequestSentError:
            marcar_feito(url, phone=clean_conta)
            return jsonify({"ok": True, "status": "pendente", "msg": "Aguardando aprovação do admin"})
        except FloodWaitError as e:
            curr_phone = f"+{clean_conta}" if clean_conta else carregar_conf().get("phone", "")
            clean_p = clean_conta or re.sub(r"[^0-9]", "", curr_phone)
            _flood_ate[curr_phone] = time.time() + e.seconds
            _flood_ate[clean_p] = time.time() + e.seconds

            # Se a aba escolheu uma conta específica no Modo Auto, não invade outra conta
            nova = None if (modo == "auto" and tel_solicitado) else rotacionar_para_proxima_conta_valida(ignorar_telefones={curr_phone, clean_p})
            if nova:
                try:
                    clean_nova = re.sub(r"[^0-9]", "", nova["telefone"])
                    c_rot = _clients_pool.get(clean_nova) or _client
                    async def _reentrar(cli=c_rot, ph=clean_nova):
                        if tipo == "invite":
                            res = await cli(ImportChatInviteRequest(valor))
                        else:
                            res = await cli(JoinChannelRequest(valor))
                        motivo_invalido = await checar_e_sair_se_sem_chat(res, url, client=cli, phone=ph, modo=modo)
                        if motivo_invalido:
                            return {
                                "ok": False, "status": "sem_chat",
                                "erro": f"Descartado ({motivo_invalido})! A conta saiu automaticamente e o grupo foi removido da lista permanentemente."
                            }
                        marcar_feito(url, phone=ph)
                        estado = carregar_estado(phone=ph)
                        salvar_estado(estado + 1, phone=ph)
                        return {"ok": True, "status": "entrou", "msg": f"Entrou com sucesso! (Alternado para {nova['nome']})", "conta_alternada": nova['nome'], "conta_ativa_real": {"phone": nova["telefone"], "nome": nova["nome"]}}
                    return jsonify(run_async(_reentrar()))
                except Exception:
                    pass

            return jsonify({
                "ok": False,
                "status": "flood",
                "reutilizar": True,
                "espera_segundos": int(e.seconds) + 5,
                "erro": f"Conta atingiu o limite temporário do próprio Telegram ({_fmt_espera(e.seconds)}). Grupo recolocado na fila!"
            }), 429
        except (UsernameNotOccupiedError, UsernameInvalidError) as e:
            marcar_feito(url, phone=clean_conta)
            return jsonify({"ok": False, "status": "expirado", "reutilizar": False, "erro": f"Grupo inexistente no Telegram ({type(e).__name__})."}), 200
        except Exception as e:
            nome = type(e).__name__
            msg_e = str(e)
            if "No user has" in msg_e or "Cannot find any entity" in msg_e or "nobody is using this username" in msg_e.lower():
                marcar_feito(url, phone=clean_conta)
                return jsonify({"ok": False, "status": "expirado", "reutilizar": False, "erro": "Link do grupo não existe mais no Telegram."}), 200
            return jsonify({"ok": False, "status": "erro_temporario", "reutilizar": True, "erro": f"{nome}: {e}"}), 200


# ══════════════════════════════════════════════════════════════════════════════
# Auto-join
# ══════════════════════════════════════════════════════════════════════════════

def _log(msg, tipo="info"):
    """Envia mensagem para a fila SSE."""
    _autojoin_queue.put(json.dumps({"tipo": tipo, "msg": msg, "ts": time.strftime("%H:%M:%S")}))


_autojoin_stats = {"concluidos": 0, "contas": []}

def _autojoin_multi_worker(telefones_lista, delay_min, delay_max, limite):
    """Roda no servidor 24h independente do navegador.
       Processa uma ou várias contas em ordem aleatória, com segurança anti-spam e anti-flood."""
    global _autojoin_running, _autojoin_stats
    _autojoin_running = True
    _autojoin_stop.clear()
    _autojoin_stats = {"concluidos": 0, "contas": telefones_lista}

    with open(GRUPOS_ARQ, encoding="utf-8") as f:
        catalogo_base = json.load(f)

    # Filtrar grupos livres de captcha / antispam
    grupos_candidatos = [g for g in catalogo_base if not eh_grupo_com_antispam(g)]

    meta_path = os.path.join(DADOS, "contas_meta.json")
    meta_dict = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f_m:
                meta_dict = json.load(f_m)
        except Exception:
            pass

    # Resolver cada conta com seu nome e cliente persistente
    contas_resolvidas = []
    for tel in telefones_lista:
        clean = re.sub(r"[^0-9]", "", str(tel))
        if not clean:
            continue
        trocar_para_telefone_sincrono(clean)
        cli = _clients_pool.get(clean) or _client
        nome_c = meta_dict.get(f"+{clean}", {}).get("nome") or meta_dict.get(clean, {}).get("nome") or f"Conta {clean[-4:]}"
        contas_resolvidas.append({"phone": f"+{clean}", "clean": clean, "nome": nome_c, "client": cli})

    if not contas_resolvidas:
        if cliente_conectado():
            cur_p = _obter_telefone_ativo()
            contas_resolvidas.append({"phone": f"+{cur_p}", "clean": cur_p, "nome": "Conta Principal", "client": _client})
        else:
            _log("❌ Nenhuma conta do Telegram conectada para o Auto-Join.", "error")
            _autojoin_running = False
            return

    nomes_fmt = ", ".join([f"<b>{c['nome']}</b>" for c in contas_resolvidas])
    _log(f"🚀 Auto-Join no Servidor 24h iniciado para: {nomes_fmt} (Ordem Aleatória | Limite: {limite}/conta)", "start")

    # Mapear grupos restantes por conta
    grupos_por_conta = {}
    for c in contas_resolvidas:
        feitos_c = carregar_todos_feitos(phone=c["clean"])
        g_livres = [g for g in grupos_candidatos if padronizar_url(g.get("url", "")) not in feitos_c]
        random.shuffle(g_livres)
        grupos_por_conta[c["clean"]] = g_livres

    total_concluido = 0

    while not _autojoin_stop.is_set():
        # Embaralhar contas ativas nesta rodada
        random.shuffle(contas_resolvidas)
        contas_ativas_nesta_rodada = 0

        for c in contas_resolvidas:
            if _autojoin_stop.is_set():
                break

            clean = c["clean"]
            entradas_hoje = carregar_estado(phone=clean)
            if entradas_hoje >= limite:
                continue

            fila_c = grupos_por_conta.get(clean, [])
            if not fila_c:
                continue

            contas_ativas_nesta_rodada += 1
            grupo = fila_c.pop(0)
            url = grupo.get("url", "")
            titulo = grupo.get("titulo") or url

            if eh_grupo_com_antispam(grupo):
                continue

            tipo, valor = normalizar_link(url)
            if not tipo:
                continue

            cli = c["client"] or _clients_pool.get(clean) or _client
            lock_c = obter_lock_conta(clean)

            _log(f"[{c['nome']}] ⚡ Entrando em: <b>{titulo}</b>...", "info")

            try:
                with lock_c:
                    async def _entrar_servidor(cli=cli, url=url, ph=clean):
                        if tipo == "invite":
                            res = await cli(ImportChatInviteRequest(valor))
                        else:
                            res = await cli(JoinChannelRequest(valor))

                        motivo_invalido = await checar_e_sair_se_sem_chat(res, url, client=cli, phone=ph, modo="auto")
                        if motivo_invalido:
                            return f"descartado:{motivo_invalido}"

                        await asyncio.sleep(1.2)
                        motivo_spam = await detectar_contra_spam_ativo(res, client=cli)
                        if motivo_spam:
                            chats = getattr(res, "chats", [])
                            if chats:
                                try:
                                    await cli(LeaveChannelRequest(chats[0]))
                                except Exception:
                                    pass
                            reclassificar_grupo_antispam(url)
                            return f"antispam:{motivo_spam}"

                        marcar_feito(url, phone=ph)
                        novo_est = carregar_estado(phone=ph) + 1
                        salvar_estado(novo_est, phone=ph)
                        return f"ok:{novo_est}"

                    res_st = run_async(_entrar_servidor(cli=cli, url=url, ph=clean))

                if res_st.startswith("descartado:"):
                    _log(f"[{c['nome']}] ⚠️ '{titulo}' descartado ({res_st.split(':', 1)[1]}).", "warn")
                elif res_st.startswith("antispam:"):
                    _log(f"[{c['nome']}] 🛡️ '{titulo}' tem Contra Spam. A conta saiu com segurança.", "warn")
                elif res_st.startswith("ok:"):
                    est_novo = int(res_st.split(":", 1)[1])
                    total_concluido += 1
                    _autojoin_stats["concluidos"] = total_concluido
                    _log(f"[{c['nome']}] ✅ Entrou em: <b>{titulo}</b> ({est_novo}/{limite} hoje)", "success")
                else:
                    _log(f"[{c['nome']}] ✓ Processado: <b>{titulo}</b>", "info")

            except UserAlreadyParticipantError:
                marcar_feito(url, phone=clean)
                _log(f"[{c['nome']}] ℹ️ Já participa de <b>{titulo}</b>", "info")
            except InviteHashExpiredError:
                remover_grupo_definitivo(url)
                marcar_feito(url, phone=clean)
                _log(f"[{c['nome']}] ⚠️ Link expirado: <b>{titulo}</b>", "warn")
            except FloodWaitError as e:
                _flood_ate[f"+{clean}"] = time.time() + e.seconds
                _flood_ate[clean] = time.time() + e.seconds
                fila_c.append(grupo)
                _log(f"[{c['nome']}] ⏳ FloodWait de {e.seconds}s pelo próprio Telegram. Grupo guardado na fila!", "warn")
            except Exception as e:
                fila_c.append(grupo)
                _log(f"[{c['nome']}] 🔄 Oscilação ao entrar em <b>{titulo}</b>: {e}. Grupo mantido na fila.", "warn")

            if _autojoin_stop.is_set():
                break

            delay_sorteado = random.randint(delay_min, delay_max)
            _log(f"⏳ Intervalo anti-spam: {delay_sorteado}s...", "wait")
            for _ in range(delay_sorteado):
                if _autojoin_stop.is_set():
                    break
                time.sleep(1)

        if contas_ativas_nesta_rodada == 0:
            _log("🏁 Todas as contas atingiram o limite diário ou a fila acabou!", "done")
            break

    _log("⏹ Auto-Join finalizado no servidor.", "done")
    _autojoin_running = False


@app.route("/api/autojoin/start", methods=["POST"])
def api_autojoin_start():
    global _autojoin_thread

    if not cliente_conectado():
        tentar_auto_conectar()
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Conta do Telegram não conectada"}), 401
    if _autojoin_running:
        return jsonify({"ok": False, "erro": "Auto-Join já está em execução no servidor"}), 400

    d = request.json or {}
    telefones = d.get("telefones") or []
    if isinstance(telefones, list):
        telefones_lista = [t.get("phone", t) if isinstance(t, dict) else str(t) for t in telefones]
    elif telefones:
        telefones_lista = [str(telefones)]
    else:
        telefones_lista = [carregar_conf().get("phone", "")]

    delay_min = int(d.get("delay_min", 10))
    delay_max = int(d.get("delay_max", 25))
    limite    = int(d.get("limite", 20))

    while not _autojoin_queue.empty():
        try:
            _autojoin_queue.get_nowait()
        except queue.Empty:
            break

    _autojoin_thread = threading.Thread(
        target=_autojoin_multi_worker,
        args=(telefones_lista, delay_min, delay_max, limite),
        daemon=True
    )
    _autojoin_thread.start()
    return jsonify({"ok": True, "msg": "Auto-Join 24h iniciado no servidor!"})


@app.route("/api/autojoin/stop", methods=["POST"])
def api_autojoin_stop():
    _autojoin_stop.set()
    return jsonify({"ok": True})


@app.route("/api/autojoin/status")
def api_autojoin_status():
    tel = str(request.args.get("telefone", "")).strip()
    clean = re.sub(r"[^0-9]", "", tel) if tel else None
    return jsonify({
        "rodando": _autojoin_running,
        "concluidos": _autojoin_stats.get("concluidos", 0),
        "contas": _autojoin_stats.get("contas", []),
        "entradas_hoje": carregar_estado(phone=clean),
        "limite_diario": int(carregar_conf().get("limite_diario", 20))
    })


@app.route("/api/autojoin/stream")
def api_autojoin_stream():
    """SSE — transmite logs do auto-join em tempo real."""
    def generator():
        try:
            yield "data: {\"tipo\":\"connected\",\"msg\":\"Conectado ao log\"}\n\n"
            while True:
                try:
                    msg = _autojoin_queue.get(timeout=15)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield "data: {\"tipo\":\"ping\"}\n\n"
        except (GeneratorExit, ConnectionResetError, BrokenPipeError, Exception):
            return

    return Response(
        generator(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


# ══════════════════════════════════════════════════════════════════════════════
# Disparador de Mensagens em Massa (Broadcast)
# ══════════════════════════════════════════════════════════════════════════════

def _broadcast_log(msg, tipo="info"):
    _broadcast_queue.put(json.dumps({
        "tipo": tipo,
        "msg": msg,
        "ts": time.strftime("%H:%M:%S")
    }))


def _obter_saudacao_atual(dt=None):
    import datetime
    if not dt:
        dt = datetime.datetime.now()
    h = dt.hour
    if 5 <= h < 12:
        return ("bom dia", "Bom dia", "BOM DIA")
    elif 12 <= h < 18:
        return ("boa tarde", "Boa tarde", "BOA TARDE")
    else:
        return ("boa noite", "Boa noite", "BOA NOITE")


def ajustar_saudacao_horario(texto, dt=None):
    if not texto:
        return ""
    p_min, p_cap, p_up = _obter_saudacao_atual(dt)
    res = texto
    # Substituir tags explícitas
    res = re.sub(r'\{Sauda[cç][aã]o\}', p_cap, res)
    res = re.sub(r'\{sauda[cç][aã]o\}', p_min, res)
    res = re.sub(r'\{tempo\}', p_min, res, flags=re.I)
    res = re.sub(r'\{periodo\}', p_min, res, flags=re.I)

    # Substituição dinâmica de saudações literais existentes mantendo o casing
    def _sub(m):
        t = m.group(0)
        if t.isupper():
            return p_up
        elif t[0].isupper():
            return p_cap
        else:
            return p_min

    padrao = re.compile(r'\b(bom\s+dia|boa\s+tarde|boa\s+noite)\b', re.I)
    res = padrao.sub(_sub, res)

    # Garantir que se a frase começa com saudação, inicie em maiúsculo
    res = res.strip()
    if res:
        for s in [p_min, "bom dia", "boa tarde", "boa noite"]:
            if res.lower().startswith(s):
                res = p_cap + res[len(s):]
                break

    return res


def _broadcast_worker(grupos_alvo, mensagem_ou_lista, delay_min=8, delay_max=25, rotacionar=True, ajustar_horario=True, client_bc=None):
    global _broadcast_running
    _broadcast_running = True
    _broadcast_stop.clear()

    total = len(grupos_alvo)

    def _limpar_parenteses(txt):
        s = (txt or "").strip()
        while s.startswith("(") and s.endswith(")"):
            s = s[1:-1].strip()
        return s

    # Processar variações de mensagens
    if isinstance(mensagem_ou_lista, list) and mensagem_ou_lista:
        variacoes = [_limpar_parenteses(m) for m in mensagem_ou_lista if str(m).strip()]
    else:
        raw = (mensagem_ou_lista or "").strip()
        if "\n---\n" in raw:
            variacoes = [_limpar_parenteses(m) for m in raw.split("\n---\n") if m.strip()]
        elif "\n" in raw:
            variacoes = [_limpar_parenteses(l) for l in raw.splitlines() if l.strip()]
        else:
            variacoes = [_limpar_parenteses(raw)] if raw else []

    if not variacoes:
        _broadcast_log("❌ Nenhuma mensagem fornecida para envio.", "error")
        _broadcast_running = False
        return

    _broadcast_log(f"🚀 Iniciando envio em massa para {total} grupo(s) com {len(variacoes)} variação(ões)...", "start")
    
    sucesso = 0
    falhas = 0

    for i, g in enumerate(grupos_alvo, 1):
        if _broadcast_stop.is_set():
            _broadcast_log("⏹ Envio em massa interrompido pelo usuário.", "warn")
            break

        chat_id = g.get("id") if isinstance(g, dict) else g
        try:
            chat_id = int(chat_id)
        except (ValueError, TypeError):
            pass
        nome = (g.get("name") if isinstance(g, dict) else None) or str(chat_id)

        # Escolher mensagem para este grupo (aleatório se rotação ligada)
        if rotacionar and len(variacoes) > 1:
            msg_base = random.choice(variacoes)
        else:
            msg_base = variacoes[(i - 1) % len(variacoes)]

        # Ajuste inteligente de saudação conforme horário do envio
        if ajustar_horario:
            msg_final = ajustar_saudacao_horario(msg_base)
        else:
            msg_final = msg_base

        preview_msg = (msg_final[:60] + "...") if len(msg_final) > 60 else msg_final
        _broadcast_log(f"[{i}/{total}] Enviando em '{nome}': \"{preview_msg}\"", "info")

        try:
            async def _enviar(cid=chat_id, msg=msg_final):
                return await (client_bc or _client).send_message(cid, msg)

            run_async(_enviar())
            sucesso += 1
            _broadcast_log(f"✅ Mensagem enviada com sucesso em: {nome} ({sucesso}/{total})", "success")

            if _broadcast_stop.is_set():
                break

            if i < total:
                # Gera delay dinâmico quebrado entre min e max (ex: 20.3s, 8.4s, nunca número fixo/robótico!)
                d_min = max(3.0, float(delay_min))
                d_max = max(d_min, float(delay_max))
                espera_quebrada = round(random.uniform(d_min, d_max), 1)
                _broadcast_log(f"⏳ Intervalo humano anti-spam: aguardando {espera_quebrada}s antes do próximo grupo...", "wait")

                tempo_restante = espera_quebrada
                while tempo_restante > 0:
                    if _broadcast_stop.is_set():
                        break
                    time.sleep(min(0.5, tempo_restante))
                    tempo_restante -= 0.5

        except FloodWaitError as e:
            _broadcast_log(f"🚦 FloodWait! Aguardando {e.seconds}s exigidos pelo Telegram...", "warn")
            if e.seconds > 600:
                _broadcast_log("❌ Tempo de FloodWait muito alto (>10min). Parando por segurança.", "error")
                break
            for _ in range(e.seconds + 5):
                if _broadcast_stop.is_set():
                    break
                time.sleep(1)

        except Exception as e:
            nome_err = type(e).__name__
            falhas += 1
            if "ChatWriteForbidden" in nome_err or "Banned" in nome_err:
                _broadcast_log(f"⚠️ Sem permissão para enviar em '{nome}' (grupo travado/apenas admins). Saindo...", "warn")
                try:
                    run_async((client_bc or _client)(LeaveChannelRequest(chat_id)))
                    _broadcast_log(f"🚪 Conta saiu automaticamente do grupo fechado: '{nome}'.", "info")
                except Exception:
                    pass
            elif "SlowModeWait" in nome_err:
                _broadcast_log(f"⏳ Grupo com SlowMode ativado: {nome}. Pulando.", "warn")
            else:
                _broadcast_log(f"❌ Erro ao enviar em '{nome}': {nome_err} ({e})", "error")

    _broadcast_log(f"🏁 Disparo concluído! Enviadas: {sucesso} | Falhas/Bloqueadas: {falhas}", "done")
    _broadcast_running = False


@app.route("/api/meus_grupos", methods=["GET", "POST"])
def api_meus_grupos():
    """Retorna exclusivamente grupos brasileiros, reais (zero canais) e com chat aberto para envio de mensagens."""
    tel_solicitado = ""
    if request.is_json and request.json:
        tel_solicitado = str(request.json.get("telefone", "")).strip()
    if not tel_solicitado:
        tel_solicitado = str(request.args.get("telefone", "")).strip()

    clean_solic = re.sub(r"[^0-9]", "", tel_solicitado) if tel_solicitado else ""
    if not clean_solic:
        conf_atual = carregar_conf()
        clean_solic = re.sub(r"[^0-9]", "", conf_atual.get("phone", ""))

    cache_file = os.path.join(DADOS, f"cache_grupos_{clean_solic}.json") if clean_solic else None
    force_refresh = (request.args.get("force") == "1") or (request.is_json and bool(request.json and request.json.get("force")))

    # 1. Se tem cache e não foi forçado refresh, responde instantaneamente do cache (< 1ms)
    if cache_file and os.path.exists(cache_file) and not force_refresh:
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cdata = json.load(f)
                c_grupos = cdata.get("grupos", [])
                if len(c_grupos) > 0:
                    return jsonify({"ok": True, "grupos": c_grupos, "total": len(c_grupos), "cached": True})
        except Exception:
            pass

    with _client_lock:
        if tel_solicitado:
            conf_atual = carregar_conf()
            clean_atual = re.sub(r"[^0-9]", "", conf_atual.get("phone", ""))
            if clean_solic != clean_atual or not cliente_conectado():
                trocar_para_telefone_sincrono(tel_solicitado)

        if not cliente_conectado():
            tentar_auto_conectar()

        if not cliente_conectado():
            if cache_file and os.path.exists(cache_file):
                try:
                    with open(cache_file, "r", encoding="utf-8") as f:
                        cdata = json.load(f)
                        return jsonify({"ok": True, "grupos": cdata.get("grupos", []), "total": len(cdata.get("grupos", [])), "cached": True})
                except Exception:
                    pass
            return jsonify({"ok": False, "erro": "Conta do Telegram não conectada"}), 401

        try:
            async def _obter_dialogs():
                grupos = []
                async for dialog in _client.iter_dialogs():
                    e = dialog.entity
                    nome = dialog.name or ""
                    username = getattr(e, "username", "") or ""

                    # 1. Zero Canais de Transmissão (apenas grupos onde mensagens podem ser trocadas)
                    if getattr(e, "broadcast", False):
                        continue
                    if isinstance(e, Channel) and not getattr(e, "megagroup", False):
                        continue
                    if not (dialog.is_group or dialog.is_channel):
                        continue

                    # 2. Chat Aberto e Liberado (membros comuns podem falar)
                    banned = getattr(e, "default_banned_rights", None)
                    if banned and getattr(banned, "send_messages", False):
                        continue

                    grupos.append({
                        "id": dialog.id,
                        "name": nome or "Grupo sem nome",
                        "username": username,
                        "unread": dialog.unread_count,
                        "membros": getattr(e, "participants_count", 0) or 0
                    })
                return grupos

            grupos = run_async(_obter_dialogs())

            # Atualizar cache em disco
            if cache_file and grupos:
                try:
                    with open(cache_file, "w", encoding="utf-8") as f:
                        json.dump({"timestamp": time.time(), "grupos": grupos}, f, ensure_ascii=False, indent=2)
                except Exception:
                    pass

            return jsonify({"ok": True, "grupos": grupos, "total": len(grupos)})
        except Exception as e:
            logger.warning(f"Erro ao obter dialogs do Telegram: {e}")
            if cache_file and os.path.exists(cache_file):
                try:
                    with open(cache_file, "r", encoding="utf-8") as f:
                        cdata = json.load(f)
                        return jsonify({"ok": True, "grupos": cdata.get("grupos", []), "total": len(cdata.get("grupos", [])), "cached": True})
                except Exception:
                    pass
            return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/limpar_canais_e_mutados", methods=["POST"])
def api_limpar_canais_e_mutados():
    """Varre a conta ativa e sai automaticamente de canais de transmissão, murais e chats fechados/mutados."""
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Conta do Telegram não conectada"}), 401

    try:
        async def _faxina():
            removidos_canais = 0
            removidos_mutados = 0
            removidos_gringos = 0
            async for dialog in _client.iter_dialogs():
                e = dialog.entity
                nome = dialog.name or ""
                username = getattr(e, "username", "") or ""

                # Sair de canais
                if getattr(e, "broadcast", False) or (isinstance(e, Channel) and not getattr(e, "megagroup", False)):
                    try:
                        await _client(LeaveChannelRequest(e))
                        removidos_canais += 1
                    except Exception:
                        pass
                    continue

                if dialog.is_group or dialog.is_channel:
                    # Sair de gringos
                    is_br, _, _ = check_brazil_confidence(nome, "", username)
                    if not is_br:
                        try:
                            await _client(LeaveChannelRequest(e))
                            removidos_gringos += 1
                        except Exception:
                            pass
                        continue

                    # Sair de grupos onde membros são proibidos de falar (mural/fechado)
                    banned = getattr(e, "default_banned_rights", None)
                    deve_sair = False
                    if banned and getattr(banned, "send_messages", False):
                        deve_sair = True
                    else:
                        try:
                            perms = await _client.get_permissions(e, "me")
                            if getattr(perms, "is_banned", False):
                                deve_sair = True
                            part = getattr(perms, "participant", None)
                            banned_p = getattr(part, "banned_rights", None)
                            if banned_p and getattr(banned_p, "send_messages", False):
                                deve_sair = True
                        except Exception:
                            pass

                    if deve_sair:
                        try:
                            await _client(LeaveChannelRequest(e))
                            removidos_mutados += 1
                        except Exception:
                            pass

            return {
                "canais_removidos": removidos_canais,
                "mutados_removidos": removidos_mutados,
                "gringos_removidos": removidos_gringos,
                "total_removidos": removidos_canais + removidos_mutados + removidos_gringos
            }

        res = run_async(_faxina())
        return jsonify({"ok": True, "detalhes": res})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/broadcast/start", methods=["POST"])
def api_broadcast_start():
    global _broadcast_thread

    d = request.json or {}
    tel_solicitado = str(d.get("telefone", "")).strip()
    if tel_solicitado:
        clean_solic = re.sub(r"[^0-9]", "", tel_solicitado)
        conf_atual = carregar_conf()
        clean_atual = re.sub(r"[^0-9]", "", conf_atual.get("phone", ""))
        if clean_solic != clean_atual or not cliente_conectado():
            trocar_para_telefone_sincrono(tel_solicitado)

    if not cliente_conectado():
        tentar_auto_conectar()
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Conta do Telegram não conectada"}), 401
    if _broadcast_running:
        return jsonify({"ok": False, "erro": "Já existe um disparo em andamento"}), 400

    d = request.json or {}
    mensagem = (d.get("mensagem") or "").strip()
    mensagens = d.get("mensagens") or []
    grupos_alvo = d.get("grupos") or []
    delay_min = int(d.get("delay_min") or d.get("delay") or 8)
    delay_max = int(d.get("delay_max") or delay_min or 25)
    if delay_max < delay_min:
        delay_max = delay_min
    rotacionar = bool(d.get("rotacionar", True))
    ajustar_horario = bool(d.get("ajustar_horario", True))

    lista_final = mensagens if (isinstance(mensagens, list) and len(mensagens) > 0) else mensagem
    if not lista_final:
        return jsonify({"ok": False, "erro": "Digite o texto da mensagem a ser enviada"}), 400
    if not grupos_alvo:
        return jsonify({"ok": False, "erro": "Selecione ao menos um grupo da lista"}), 400

    # Limpar fila de log antiga
    while not _broadcast_queue.empty():
        try:
            _broadcast_queue.get_nowait()
        except queue.Empty:
            break

    c_disparo = _clients_pool.get(re.sub(r"[^0-9]", "", tel_solicitado)) if tel_solicitado else _client
    if not c_disparo:
        c_disparo = _client
    _broadcast_thread = threading.Thread(
        target=_broadcast_worker,
        args=(grupos_alvo, lista_final, delay_min, delay_max, rotacionar, ajustar_horario, c_disparo),
        daemon=True
    )
    _broadcast_thread.start()
    return jsonify({"ok": True})


@app.route("/api/broadcast/gerar_ia", methods=["POST"])
def api_broadcast_gerar_ia():
    """Gera 10 mensagens humanizadas com saudações dinâmicas de acordo com o horário."""
    d = request.json or {}
    estilo = d.get("estilo", "pv")
    prompt = (d.get("prompt") or "").strip()

    # Banco especializado de 10+ variações por estilo
    banco_ia = {
        "pv": [
            "Oiie, {saudacao} vem pv! 💞",
            "Oiie {saudacao}, me chama rs🙈",
            "{Saudacao}, Me chama vaii🥰",
            "Oii, pv liberado!",
            "Hoje onde? rsrs",
            "Oie gente, {saudacao}! Quem tiver acordado chama no pv ✨",
            "Oii, alguém aí pra conversar? Me chama lá no pv rs 💬",
            "Passando pra desejar {saudacao}! Vem de pv bater papo 💖",
            "Alguém online agora? Me chama no privado 😉",
            "{Saudacao} pessoal! Pv tá abertinho pra resenha 🔥",
            "Oii amor, me dá um oi no pv? 🥰",
            "Oiie, sem sono por aqui rs, quem anima bater papo no pv? 🌙",
            "Bora conversar? Pv liberado pra quem for gente boa 💫",
            "{Saudacao}! Quem tiver afim de papear dá um alô no pv 💌"
        ],
        "amizade": [
            "{Saudacao} galera! Quem anima fazer amizade nova hoje? 🤝",
            "Oii pessoal, passando pra desejar {saudacao} a todos! ✨",
            "Resenha boa! Quem for do Brasil chama no pv pra papear 🇧🇷",
            "{Saudacao} grupo! Como tá o dia de vocês por aí? ☕",
            "Oie, quem estiver de bobeira chama lá no privado rs 💬",
            "Passando pra marcar presença no grupo, {saudacao} pra geral! ✌️",
            "Alguém aí online pra papear e jogar conversa fora? Chama pv! 🔥",
            "{Saudacao} turma! Quem tiver de boa manda um alô 🚀",
            "Sempre bom conhecer gente legal aqui, pv tá aberto! 🌟",
            "{Saudacao} a todos do grupo, um ótimo dia pra nós! ☀️"
        ],
        "divulgacao": [
            "{Saudacao} pessoal! Vim trazer uma novidade que acabou de sair 🚀",
            "Opa, {saudacao}! Não percam esse conteúdo novo liberado hoje 🔥",
            "{Saudacao} grupo! Quem curte novidades exclusivas me chama no pv 💎",
            "Fala galera, {saudacao}! Liberamos vagas limitadas agora, confira! ⚡",
            "{Saudacao} a todos! Conteúdo especial no ar pra vocês 🎯",
            "Atenção pessoal: oportunidade imperdível pra hoje, {saudacao}! 💥",
            "Passando pra avisar que tem novidade fresquinha, me chama no pv ✨",
            "{Saudacao} pessoal, confiram o que preparamos pra vocês hoje! 🌟",
            "Quem quiser conferir de primeira mão me dá um oi no privado 📩",
            "{Saudacao} galera! Acessem enquanto ainda tá disponível ⏳"
        ],
        "vendas": [
            "{Saudacao}! Ofertas imperdíveis liberadas hoje com super desconto 🛍️",
            "Oii gente, {saudacao}! Promoção relâmpago acontecendo agora ⚡",
            "{Saudacao} pessoal! Melhores condições do dia no ar, corre ver 💰",
            "Passando pra avisar dos novos produtos disponíveis hoje, {saudacao}! 📦",
            "{Saudacao}! Quem quiser aproveitar o cupom especial chama no pv 🏷️",
            "Oferta exclusiva pra galera do grupo, aproveitem hoje! 🔥",
            "{Saudacao} grupo! Últimas unidades com frete grátis liberadas 🚚",
            "Oportunidade única de economia pra vocês hoje, {saudacao}! 💸",
            "Chama no pv pra garantir sua condição especial antes que acabe! 📲",
            "{Saudacao}! Descontos de até 50% válidos somente hoje 🎉"
        ]
    }

    if prompt:
        assunto = prompt
        mensagens = [
            f"Oie pessoal, {{saudacao}}! Alguém interessado em {assunto}? Me chama no pv 🚀",
            f"{{Saudacao}} galera! Quem quiser saber mais sobre {assunto} dá um alô no privado 💬",
            f"Passando pra compartilhar sobre {assunto}, {{saudacao}} a todos! ✨",
            f"Oii, {{saudacao}}! Liberamos novidades sobre {assunto}, pv tá aberto 🔥",
            f"{{Saudacao}}! Quem aí curte {assunto}? Vem de pv bater um papo 🤝",
            f"Alguém por aqui procurando sobre {assunto}? Me chama no privado 😉",
            f"{{Saudacao}} grupo! Dica de ouro sobre {assunto} pra vocês hoje 💡",
            f"Opa, {{saudacao}}! Mais alguém ligado em {assunto}? Me manda mensagem 📲",
            f"{{Saudacao}} pessoal! Informações exclusivas de {assunto} no pv 💎",
            f"Hoje liberamos novidades sobre {assunto}, {{saudacao}} a todos! 🎉"
        ]
    else:
        opcoes = banco_ia.get(estilo, banco_ia["pv"])
        if len(opcoes) >= 10:
            mensagens = random.sample(opcoes, 10)
        else:
            mensagens = opcoes

    def _limpar_parenteses(txt):
        s = (txt or "").strip()
        while s.startswith("(") and s.endswith(")"):
            s = s[1:-1].strip()
        return s

    p_min, p_cap, _ = _obter_saudacao_atual()
    # Todas as mensagens e prévias saem 100% adaptadas para o horário atual
    mensagens_ajustadas = [_limpar_parenteses(ajustar_saudacao_horario(m)) for m in mensagens]

    return jsonify({
        "ok": True,
        "estilo": estilo,
        "saudacao_atual": p_cap,
        "mensagens": mensagens_ajustadas,
        "previas": mensagens_ajustadas
    })


@app.route("/api/broadcast/stop", methods=["POST"])
def api_broadcast_stop():
    _broadcast_stop.set()
    return jsonify({"ok": True})


@app.route("/api/broadcast/status")
def api_broadcast_status():
    return jsonify({"rodando": _broadcast_running})


@app.route("/api/broadcast/stream")
def api_broadcast_stream():
    """SSE — transmite logs do broadcast em tempo real."""
    def generator():
        try:
            yield "data: {\"tipo\":\"connected\",\"msg\":\"Conectado ao log do disparador\"}\n\n"
            while True:
                try:
                    msg = _broadcast_queue.get(timeout=15)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield "data: {\"tipo\":\"ping\"}\n\n"
        except (GeneratorExit, ConnectionResetError, BrokenPipeError, Exception):
            return

    return Response(
        generator(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


@app.route("/api/sair_chat", methods=["POST"])
def api_sair_chat():
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Conta não conectada"}), 401
    d = request.json or {}
    chat_ident = d.get("chat")
    if not chat_ident:
        return jsonify({"ok": False, "erro": "Chat não especificado"}), 400
    try:
        async def _sair():
            from telethon.tl.functions.channels import LeaveChannelRequest
            entity = await _client.get_entity(chat_ident)
            await _client(LeaveChannelRequest(entity))
            return True
        run_async(_sair())
        return jsonify({"ok": True, "msg": f"Saiu do chat {chat_ident} com sucesso!"})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/sair_e_limpar_bloqueados", methods=["POST"])
def api_sair_e_limpar_bloqueados():
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Conta não conectada"}), 401
    
    try:
        async def _limpar():
            from telethon.tl.functions.channels import LeaveChannelRequest
            removidos = []
            async for dialog in _client.iter_dialogs():
                e = dialog.entity
                if not (dialog.is_group or dialog.is_channel):
                    continue
                is_broadcast = getattr(e, "broadcast", False)
                banned_def = getattr(e, "default_banned_rights", None)
                def_send_block = bool(banned_def and getattr(banned_def, "send_messages", False))
                
                # Se for broadcast ou se envio de mensagens for bloqueado por padrao
                if is_broadcast or def_send_block:
                    try:
                        await _client(LeaveChannelRequest(e))
                    except Exception:
                        pass
                    u = getattr(e, "username", "") or ""
                    nome = dialog.name or ""
                    removidos.append({"name": nome, "username": u, "motivo": "broadcast" if is_broadcast else "sem_chat"})
                    if u:
                        remover_grupo_definitivo("https://t.me/" + u)
            return removidos

        res = run_async(_limpar())
        
        # Atualizar br_validados.json removendo nomes/links correspondentes
        with open(GRUPOS_ARQ, "r", encoding="utf-8") as f:
            grupos = json.load(f)
        
        nomes_remover = {r["name"].lower() for r in res}
        users_remover = {r["username"].lower() for r in res if r["username"]}
        
        novos = []
        for g in grupos:
            u = (g.get("url") or "").lower()
            tit = (g.get("titulo") or "").lower()
            deve_remover = False
            for un in users_remover:
                if un in u:
                    deve_remover = True
                    break
            if tit in nomes_remover:
                deve_remover = True
            if not deve_remover:
                novos.append(g)
            
        with open(GRUPOS_ARQ, "w", encoding="utf-8") as f:
            json.dump(novos, f, ensure_ascii=False, indent=2)

        return jsonify({"ok": True, "removidos": res, "total_removidos": len(res), "restantes_no_banco": len(novos)})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/diagnosticar_chats")
def api_diagnosticar_chats():
    if not cliente_conectado():
        return jsonify({"ok": False, "erro": "Não conectado"}), 401
    
    try:
        async def _diag():
            resultados = []
            async for dialog in _client.iter_dialogs():
                e = dialog.entity
                if not (dialog.is_group or dialog.is_channel):
                    continue
                
                is_broadcast = getattr(e, "broadcast", False)
                banned_def = getattr(e, "default_banned_rights", None)
                def_send_block = bool(banned_def and getattr(banned_def, "send_messages", False))
                
                # Permissão real do usuário conectado
                pode_enviar = False
                motivo = "ok"
                perms_repr = ""
                try:
                    perms = await _client.get_permissions(e, "me")
                    perms_repr = f"admin={perms.is_admin}, banned={perms.is_banned}, has_left={perms.has_left}, send_messages={getattr(perms, 'send_messages', None)}, can_send_messages={getattr(perms, 'can_send_messages', None)}"
                    # Se nao for banido e o grupo nao for broadcast nem default banned
                    if not is_broadcast and not def_send_block and not perms.is_banned:
                        # Se can_send_messages ou send_messages for True ou se default nao bloquear
                        pode_enviar = True
                        motivo = "liberado"
                except Exception as ex:
                    motivo = f"erro_perm: {ex}"
                
                if is_broadcast:
                    pode_enviar = False
                    motivo = "canal_broadcast"
                elif def_send_block:
                    pode_enviar = False
                    motivo = "grupo_fechado_default_banned"

                resultados.append({
                    "id": dialog.id,
                    "name": dialog.name,
                    "username": getattr(e, "username", ""),
                    "is_broadcast": is_broadcast,
                    "def_send_block": def_send_block,
                    "pode_enviar": pode_enviar,
                    "motivo": motivo,
                    "perms_detail": perms_repr
                })
            return resultados

        res = run_async(_diag())
        return jsonify({"ok": True, "chats": res})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/desconectar", methods=["POST"])
def api_desconectar():
    global _client
    if _client:
        try:
            run_async(_client.disconnect())
        except Exception:
            pass
        _client = None
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    garantir_loop()
    print("\n" + "═"*55)
    print("  🇧🇷 Dashboard Telegram BR")
    print("  http://localhost:5000")
    print("═"*55 + "\n")
    while True:
        try:
            port = int(os.environ.get("PORT", 5000))
            app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
        except Exception as e:
            print(f"Erro no servidor: {e}. Reiniciando em 2 segundos...")
            time.sleep(2)
