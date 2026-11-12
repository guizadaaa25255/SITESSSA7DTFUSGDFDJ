# -*- coding: utf-8 -*-
"""
pipeline/storage/database.py
Camada de persistência em SQLite para gerenciar links descobertos,
grupos validados, descartes com motivo e checkpoints de execução.
"""

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DB_PATH_DEFAULT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "dados",
    "grupos_telegram.db"
)


class TelegramDatabase:
    def __init__(self, db_path: str = DB_PATH_DEFAULT):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def init_db(self):
        with self.get_connection() as conn:
            # 1. Tabela de grupos válidos (100% aprovados em todos os filtros)
            conn.execute("""
            CREATE TABLE IF NOT EXISTS grupos_validados (
                link_convite TEXT PRIMARY KEY,
                username TEXT,
                nome TEXT NOT NULL,
                descricao TEXT,
                membros INTEGER,
                tipo TEXT DEFAULT 'grupo',
                idioma_detectado TEXT,
                score_brasil REAL,
                categoria TEXT,
                fonte_de_origem TEXT,
                data_coleta TEXT,
                imagem TEXT
            );
            """)

            # 2. Tabela de links descartados com auditoria do motivo
            conn.execute("""
            CREATE TABLE IF NOT EXISTS descartados (
                link_convite TEXT PRIMARY KEY,
                motivo TEXT NOT NULL,
                detalhes TEXT,
                data_descarte TEXT
            );
            """)

            # 3. Fila de links descobertos
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fila_descoberta (
                link_convite TEXT PRIMARY KEY,
                fonte TEXT,
                status TEXT DEFAULT 'pendente',
                data_descoberta TEXT
            );
            """)

            # Índices de performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_grupos_categoria ON grupos_validados(categoria);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_grupos_membros ON grupos_validados(membros);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_descartados_motivo ON descartados(motivo);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fila_status ON fila_descoberta(status);")

    def add_discovered_links(self, links_with_sources: List[Tuple[str, str]]) -> int:
        """Adiciona em lote novos links descobertos na fila (ignora os já existentes)."""
        now = datetime.now().isoformat()
        novos = 0
        with self.get_connection() as conn:
            cursor = conn.cursor()
            for link, fonte in links_with_sources:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO fila_descoberta (link_convite, fonte, status, data_descoberta)
                        VALUES (?, ?, 'pendente', ?)
                    """, (link, fonte, now))
                    if cursor.rowcount > 0:
                        novos += 1
                except Exception:
                    pass
        return novos

    def get_pending_links(self, limit: int = 1000) -> List[Tuple[str, str]]:
        """Recupera um lote de links pendentes para validação."""
        with self.get_connection() as conn:
            cursor = conn.execute("""
                SELECT link_convite, fonte FROM fila_descoberta
                WHERE status = 'pendente'
                LIMIT ?
            """, (limit,))
            return [(row["link_convite"], row["fonte"]) for row in cursor.fetchall()]

    def is_known(self, link: str) -> bool:
        """Verifica se o link já foi validado ou descartado anteriormente."""
        with self.get_connection() as conn:
            r1 = conn.execute("SELECT 1 FROM grupos_validados WHERE link_convite = ? LIMIT 1", (link,)).fetchone()
            if r1:
                return True
            r2 = conn.execute("SELECT 1 FROM descartados WHERE link_convite = ? LIMIT 1", (link,)).fetchone()
            if r2:
                return True
            return False

    def save_validated_group(self, grupo: Dict[str, Any]):
        """Salva um grupo que passou com sucesso em todos os filtros."""
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO grupos_validados (
                    link_convite, username, nome, descricao, membros,
                    tipo, idioma_detectado, score_brasil, categoria,
                    fonte_de_origem, data_coleta, imagem
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                grupo["link_convite"],
                grupo.get("username"),
                grupo["nome"],
                grupo.get("descricao", ""),
                grupo.get("membros", 0),
                grupo.get("tipo", "grupo"),
                grupo.get("idioma_detectado", "pt"),
                grupo.get("score_brasil", 1.0),
                grupo.get("categoria", "geral"),
                grupo.get("fonte_de_origem", "desconhecido"),
                grupo.get("data_coleta", datetime.now().isoformat()),
                grupo.get("imagem", "")
            ))
            conn.execute("UPDATE fila_descoberta SET status = 'processado' WHERE link_convite = ?", (grupo["link_convite"],))

    def save_discarded(self, link: str, motivo: str, detalhes: str = ""):
        """Registra o descarte de um link com o respectivo motivo."""
        now = datetime.now().isoformat()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO descartados (link_convite, motivo, detalhes, data_descarte)
                VALUES (?, ?, ?, ?)
            """, (link, motivo, detalhes, now))
            conn.execute("UPDATE fila_descoberta SET status = 'descartado' WHERE link_convite = ?", (link,))

    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas detalhadas da base."""
        with self.get_connection() as conn:
            total_validos = conn.execute("SELECT COUNT(*) FROM grupos_validados").fetchone()[0]
            total_descartados = conn.execute("SELECT COUNT(*) FROM descartados").fetchone()[0]
            total_pendentes = conn.execute("SELECT COUNT(*) FROM fila_descoberta WHERE status = 'pendente'").fetchone()[0]
            total_fila = conn.execute("SELECT COUNT(*) FROM fila_descoberta").fetchone()[0]

            motivos_cur = conn.execute("SELECT motivo, COUNT(*) as qtd FROM descartados GROUP BY motivo")
            descartes_por_motivo = {row["motivo"]: row["qtd"] for row in motivos_cur.fetchall()}

            cats_cur = conn.execute("SELECT categoria, COUNT(*) as qtd FROM grupos_validados GROUP BY categoria ORDER BY qtd DESC")
            categorias_count = {row["categoria"]: row["qtd"] for row in cats_cur.fetchall()}

            return {
                "total_validos": total_validos,
                "total_descartados": total_descartados,
                "total_pendentes": total_pendentes,
                "total_descobertos": total_fila,
                "descartes_por_motivo": descartes_por_motivo,
                "categorias": categorias_count,
            }

    def get_all_validated(self) -> List[Dict[str, Any]]:
        """Recupera todos os grupos validados para exportação."""
        with self.get_connection() as conn:
            cur = conn.execute("""
                SELECT link_convite, username, nome, descricao, membros, tipo,
                       idioma_detectado, score_brasil, categoria, fonte_de_origem, data_coleta, imagem
                FROM grupos_validados
                ORDER BY membros DESC
            """)
            return [dict(row) for row in cur.fetchall()]
