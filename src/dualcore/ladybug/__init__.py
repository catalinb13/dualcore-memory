"""Ladybug — Reactive/Episodic memory core (Dynamic Truth).

SQLite-backed versioned memory store with Multi-Fidelity Semantic Bridge.
Each entry has:
  - Operational Layer: metadata tags + importance score (fast, deterministic)
  - Cognitive Layer: nuance field (full uncompressed rationale, deep recall)

Ladybug captures what is BEING SAID — session facts, recent decisions,
shifting preferences. It is the "flow" of the current interaction.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 base_id TEXT NOT NULL,
 version INTEGER NOT NULL DEFAULT 1,
 content TEXT NOT NULL,
 memory_type TEXT NOT NULL DEFAULT 'general',
 importance INTEGER NOT NULL DEFAULT 5,
 metadata TEXT NOT NULL DEFAULT '{}',
 nuance TEXT NOT NULL DEFAULT '',
 created_at TEXT NOT NULL DEFAULT (datetime('now')),
 updated_at TEXT NOT NULL DEFAULT (datetime('now')),
 session_id TEXT NOT NULL DEFAULT '',
 source TEXT,
 target TEXT
);
CREATE INDEX IF NOT EXISTS idx_base_id ON memories(base_id);
CREATE INDEX IF NOT EXISTS idx_importance ON memories(importance);
CREATE INDEX IF NOT EXISTS idx_type ON memories(memory_type);
CREATE INDEX IF NOT EXISTS idx_created ON memories(created_at);
CREATE INDEX IF NOT EXISTS idx_updated ON memories(updated_at);
CREATE INDEX IF NOT EXISTS idx_base_ver ON memories(base_id, version);
CREATE INDEX IF NOT EXISTS idx_source ON memories(source);
CREATE INDEX IF NOT EXISTS idx_target ON memories(target);
"""


class LadybugStore:
    """Thread-safe SQLite store for episodic memory entries."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SCHEMA)
        
        # Migration: add source/target columns and updated_at if missing
        try:
            cursor = self._conn.execute("PRAGMA table_info(memories)")
            columns = [row["name"] for row in cursor.fetchall()]
            
            # 1. Source/Target Migration
            if "source" not in columns:
                self._conn.execute("ALTER TABLE memories ADD COLUMN source TEXT")
                logger.info("Added 'source' column to memories table.")
            if "target" not in columns:
                self._conn.execute("ALTER TABLE memories ADD COLUMN target TEXT")
                logger.info("Added 'target' column to memories table.")
            
            if "source" in columns or "target" in columns:
                 self._conn.execute(
                     "UPDATE memories SET source = json_extract(metadata, '$.source'), "
                     "target = json_extract(metadata, '$.target') WHERE source IS NULL OR target IS NULL"
                 )

            # 2. updated_at Migration
            if "updated_at" not in columns:
                self._conn.execute("ALTER TABLE memories ADD COLUMN updated_at TEXT DEFAULT (datetime('now'))")
                # Backfill updated_at with created_at for existing rows
                self._conn.execute("UPDATE memories SET updated_at = created_at WHERE updated_at IS NULL")
                self._conn.execute("CREATE INDEX IF NOT EXISTS idx_updated ON memories(updated_at)")
                logger.info("Added 'updated_at' column and index to memories table.")

            self._conn.commit()
            logger.info("Ladybug schema migration complete.")
        except Exception as e:
            logger.warning("Migration failed (might already be migrated): %s", e)

        logger.debug("Ladybug store initialized at %s", db_path)

    def store(
        self,
        content: str,
        base_id: Optional[str] = None,
        version: int = 1,
        memory_type: str = "general",
        importance: int = 5,
        metadata: Optional[Dict] = None,
        nuance: str = "",
        session_id: str = "",
    ) -> Dict[str, Any]:
        """Store a new memory entry. Returns dict with id, base_id, version."""
        if base_id is None:
            base_id = str(uuid.uuid4())[:8]
        
        meta_dict = metadata or {}
        meta_json = json.dumps(meta_dict)
        source = meta_dict.get("source")
        target = meta_dict.get("target")
        now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        with self._lock:
            cur = self._conn.execute(
                """INSERT INTO memories (base_id, version, content, memory_type,
                   importance, metadata, nuance, session_id, source, target, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (base_id, version, content, memory_type, importance,
                 meta_json, nuance, session_id, source, target, now),
            )
            self._conn.commit()
            return {
                "id": cur.lastrowid,
                "base_id": base_id,
                "version": version,
            }

    def get_entry(self, entry_id: int) -> Optional[Dict[str, Any]]:
        """Get a single entry by row id."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM memories WHERE id = ?", (entry_id,)
            ).fetchone()
        if not row:
            return None
        return self._row_to_dict(row)

    def get_latest_entries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get latest version of each base_id, sorted by importance then recency."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT m.* FROM memories m
                INNER JOIN (
                    SELECT base_id, MAX(version) as max_ver
                    FROM memories GROUP BY base_id
                ) latest ON m.base_id = latest.base_id AND m.version = latest.max_ver
                ORDER BY m.importance DESC, m.created_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_changes_since(self, timestamp: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent changes (new or updated entries) since the given timestamp."""
        with self._lock:
            rows = self._conn.execute("""
                SELECT m.* FROM memories m
                INNER JOIN (
                    SELECT base_id, MAX(version) as max_ver
                    FROM memories GROUP BY base_id
                ) latest ON m.base_id = latest.base_id AND m.version = latest.max_ver
                WHERE m.updated_at > ?
                ORDER BY m.updated_at DESC
                LIMIT ?
            """, (timestamp, limit)).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def search(self, query: str, limit: int = 8) -> List[Dict[str, Any]]:
        """Simple keyword search across content and nuance fields.

        Escapes LIKE metacharacters (%, _) in the query to prevent
        injection-style pattern matching.
        """
        # Escape LIKE metacharacters to prevent injection
        safe_query = query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"%{safe_query}%"
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM memories
                WHERE content LIKE ? ESCAPE '\\' OR nuance LIKE ? ESCAPE '\\'
                ORDER BY importance DESC, created_at DESC
                LIMIT ?""",
                (pattern, pattern, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def find_by_metadata(self, source: str, target: str,
                         limit: int = 5) -> List[Dict[str, Any]]:
        """Find entries by metadata source and target fields.

        Uses indexed columns for fast lookup.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM memories
                WHERE source = ? AND target = ?
                ORDER BY created_at DESC
                LIMIT ?""",
                (source, target, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def find_by_metadata_key(self, mirror_key: str, limit: int = 10) -> list:
        """Find entries by mirror_key in metadata JSON.

        mirror_key is a unique composite key (target::content_hash) that
        avoids aliasing when multiple entries share the same target category.
        """
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM memories WHERE json_extract(metadata, '$.mirror_key') = ? "
                "ORDER BY importance DESC, created_at DESC LIMIT ?",
                (mirror_key, limit),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def count_entries(self) -> int:
        """Count distinct base_ids (unique memory topics)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(DISTINCT base_id) FROM memories"
            ).fetchone()
        return row[0] if row else 0

    def get_history(self, base_id: str) -> List[Dict[str, Any]]:
        """Get full version history for a base_id."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM memories WHERE base_id = ? ORDER BY version ASC",
                (base_id,),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def delete_base(self, base_id: str) -> bool:
        """Delete all versions of a base_id."""
        with self._lock:
            count = self._conn.execute(
                "SELECT COUNT(*) FROM memories WHERE base_id = ?", (base_id,)
            ).fetchone()[0]
            self._conn.execute(
                "DELETE FROM memories WHERE base_id = ?", (base_id,)
            )
            self._conn.commit()
        return count > 0

    def store_versioned(
        self,
        entry_id: int,
        new_content: str,
        metadata: Optional[Dict] = None,
        nuance: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Create a new version of an existing entry (append-only versioning)."""
        existing = self.get_entry(entry_id)
        if not existing:
            return None
        return self.store(
            content=new_content,
            base_id=existing["base_id"],
            version=existing["version"] + 1,
            memory_type=existing["memory_type"],
            importance=existing["importance"],
            metadata=metadata or existing.get("metadata", {}),
            nuance=nuance or existing.get("nuance", ""),
            session_id=existing.get("session_id", ""),
        )

    def get_recent_claims(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent entries suitable for verification against Enzyme.

        Returns entries with importance >= 4, sorted by recency.
        These are the claims most likely to conflict with workspace knowledge.
        """
        with self._lock:
            rows = self._conn.execute(
                """SELECT m.* FROM memories m
                   INNER JOIN (
                       SELECT base_id, MAX(version) as max_ver
                       FROM memories GROUP BY base_id
                   ) latest ON m.base_id = latest.base_id AND m.version = latest.max_ver
                   WHERE m.importance >= 4
                   ORDER BY m.created_at DESC
                   LIMIT ?""",
                (limit,),
            ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def prune_old_entries(self, max_age_days: int = 90,
                          keep_importance: int = 7) -> int:
        """Delete stale entries older than max_age_days.

        Entries with importance >= keep_importance are preserved regardless
        of age (they represent hard-won knowledge that should not auto-expire).
        Returns the number of deleted base groups.
        """
        try:
            cutoff_date = (datetime.utcnow() - timedelta(days=max_age_days)).strftime(
                '%Y-%m-%d %H:%M:%S'
            )
        except Exception:
            cutoff_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with self._lock:
            # Find base_ids where ALL versions are old AND low-importance
            rows = self._conn.execute("""
                SELECT base_id FROM memories
                WHERE created_at < ? AND importance < ?
                AND base_id NOT IN (
                    SELECT base_id FROM memories
                    WHERE importance >= ? OR created_at >= ?
                )
                GROUP BY base_id
            """, (cutoff_date, keep_importance, keep_importance, cutoff_date)).fetchall()
            base_ids = [r[0] for r in rows]
            if not base_ids:
                return 0
            placeholders = ','.join('?' * len(base_ids))
            self._conn.execute(
                f"DELETE FROM memories WHERE base_id IN ({placeholders})",
                base_ids,
            )
            self._conn.commit()
            logger.info("Pruned %d stale memory groups (age > %d days, importance < %d)",
                        len(base_ids), max_age_days, keep_importance)
            return len(base_ids)

    def close(self):
        """Close the database connection. Thread-safe."""
        with self._lock:
            self._conn.close()

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
        d = dict(row)
        try:
            if isinstance(d.get("metadata"), str):
                d["metadata"] = json.loads(d.get("metadata", "{}"))
        except (json.JSONDecodeError, TypeError):
            d["metadata"] = {}
        return d
