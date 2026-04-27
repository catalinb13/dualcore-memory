from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

class EnzymeBridge(abc.ABC):
    """
    Abstract Base Class for a 'Static Truth' bridge.
    
    Implementations of this class connect the Reconciliation Engine
    to a structured workspace knowledge source (e.g., enzyme CLI,
    vector databases, or filesystem indices).
    """

    @property
    @abc.abstractmethod
    def available(self) -> bool:
        """Returns True if the bridge is ready to be used for queries."""
        pass

    @abc.abstractmethod
    def ensure_initialized(self) -> bool:
        """Bootstrap the bridge if needed. Returns True if ready."""
        pass

    @abc.abstractmethod
    def petri(self, query: Optional[str] = None, top: int = 10) -> Dict[str, Any]:
        """
        Retrieve a ranked overview of entities in the workspace.
        
        Returns a dictionary, ideally containing an 'entities' key:
        List[Dict[str, str]] = [{'name': '...', 'file_path': '...'}, ...]
        """
        pass

    @abc.abstractmethod
    def catalyze(self, query: str, limit: int = 10,
                 register: str = "explore") -> Dict[str, Any]:
        """
        Perform a semantic search/concept-level comparison.
        
        Returns a dictionary, ideally containing a 'results' key:
        List[Dict[str, Any]] = [{'content': '...', 'similarity': 0.9, ...}, ...]
        """
        pass

    @abc.abstractmethod
    def refresh(self, full: bool = False) -> Dict[str, Any]:
        """Trigger a re-indexing of the workspace."""
        pass

    @abc.abstractmethod
    def status(self) -> Dict[str, Any]:
        """Return current health and statistics of the bridge."""
        pass
