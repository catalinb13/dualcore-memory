from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from typing import Any, Dict, List, Optional

from .base import EnzymeBridge

logger = logging.getLogger(__name__)


class EnzymeCLIBridge(EnzymeBridge):
    """
    Concrete implementation of EnzymeBridge that wraps the 'enzyme' CLI tool.
    """

    def __init__(self, vault_path: Optional[str] = None):
        self._vault_path = vault_path
        self._available: Optional[bool] = None  # None = unchecked
        self._initialized = False

    @property
    def available(self) -> bool:
        if self._available is None:
            self._available = shutil.which("enzyme") is not None
        return self._available

    def ensure_initialized(self) -> bool:
        """Bootstrap enzyme if needed. Returns True if ready for queries."""
        if self._initialized:
            return True
        if not self.available:
            return False

        vault = self._vault_path
        db_path = os.path.join(vault, ".enzyme", "enzyme.db") if vault else ".enzyme/enzyme.db"

        if not os.path.exists(db_path):
            try:
                cmd = ["enzyme", "init"]
                if vault:
                    cmd = ["enzyme", "init", "-p", vault]
                subprocess.run(cmd, capture_output=True, timeout=120)
            except (subprocess.TimeoutExpired, FileNotFoundError) as e:
                logger.warning("Enzyme init failed: %s", e)
                return False
        else:
            try:
                cmd = ["enzyme", "refresh", "--quiet"]
                if vault:
                    cmd = ["enzyme", "refresh", "--quiet", "-p", vault]
                subprocess.run(cmd, capture_output=True, timeout=60)
            except subprocess.TimeoutExpired:
                logger.warning("Enzyme refresh timed out (60s) — stale index may be used")
            except FileNotFoundError:
                pass 

        self._initialized = True
        return True

    def petri(self, query: Optional[str] = None, top: int = 10) -> Dict[str, Any]:
        if not self.ensure_initialized():
            return {"error": "enzyme not available or not initialized"}
        cmd = ["enzyme", "petri", "-n", str(top)]
        if self._vault_path:
            cmd.extend(["-p", self._vault_path])
        if query:
            cmd.extend(["--query", query])
        return self._run(cmd)

    def catalyze(self, query: str, limit: int = 10,
                 register: str = "explore") -> Dict[str, Any]:
        if not self.ensure_initialized():
            return {"error": "enzyme not available or not initialized"}
        cmd = ["enzyme", "catalyze", query, "-n", str(limit)]
        if self._vault_path:
            cmd.extend(["-p", self._vault_path])
        if register != "explore":
            cmd.extend(["--register", register])
        return self._run(cmd)

    def refresh(self, full: bool = False) -> Dict[str, Any]:
        if not self.available:
            return {"error": "enzyme binary not found"}
        cmd = ["enzyme", "refresh", "--quiet"]
        if self._vault_path:
            cmd.extend(["-p", self._vault_path])
        if full:
            cmd.append("--full")
        return self._run(cmd, timeout=120)

    def status(self) -> Dict[str, Any]:
        if not self.available:
            return {"error": "enzyme binary not found"}
        cmd = ["enzyme", "status"]
        if self._vault_path:
            cmd.extend(["-p", self._vault_path])
        return self._run(cmd)

    def _run(self, args: List[str], timeout: int = 30) -> Dict[str, Any]:
        try:
            result = subprocess.run(
                args, capture_output=True, text=True, timeout=timeout
            )
            if result.returncode != 0:
                return {"error": result.stderr.strip() or f"enzyme exited with code {result.returncode}"}
            output = result.stdout.strip()
            if not output:
                return {"ok": True}
            try:
                return json.loads(output)
            except json.JSONDecodeError:
                return {"output": output}
        except subprocess.TimeoutExpired:
            return {"error": f"enzyme timed out after {timeout}s"}
        except FileNotFoundError:
            return {"error": "enzyme binary not found"}
