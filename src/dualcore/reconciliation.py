"""Reconciliation Engine — Prefetch-as-Reconciliation.

Bridges Ladybug (dynamic/session truth) with Enzyme (static/workspace truth).
Runs automatically on prefetch(), surfacing conflicts without the LLM
needing to explicitly call any tool.

Multi-Fidelity Semantic Bridge:
  - Operational Layer: deterministic tag matching (fast, exact)
  - Cognitive Layer: concept-level comparison via enzyme catalyze (slow, deep)

The engine runs in two phases:
  Phase 1 (Operational): Extract keywords from Ladybug entries, compare
    against Enzyme petri entities. Flag exact/near matches as "verified" or
    "tension" based on consistency. Fast — runs every prefetch.
  Phase 2 (Cognitive): On HIGH importance entries, run enzyme catalyze
    to check if workspace knowledge contradicts the Ladybug claim.
    Slower — only runs for importance >= 7 or on explicit verify requests.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from .ladybug import LadybugStore
from .enzyme import EnzymeBridge

logger = logging.getLogger(__name__)


class ReconciliationStatus(str, Enum):
    VERIFIED = "verified"       # Ladybug claim matches Enzyme knowledge
    TENSION = "tension"         # Ladybug claim partially contradicts Enzyme
    UNVERIFIED = "unverified"   # No Enzyme data to cross-reference
    CONFLICT = "conflict"       # Ladybug claim directly contradicts Enzyme


@dataclass
class ReconciliationResult:
    """Single claim reconciliation outcome."""
    claim: str
    status: ReconciliationStatus
    evidence: str = ""
    source_entries: List[str] = field(default_factory=list)
    importance: int = 5


class ReconciliationEngine:
    """Cross-core conflict detection between Ladybug and Enzyme."""

    def __init__(self, ladybug: LadybugStore, enzyme: EnzymeBridge):
        self._ladybug = ladybug
        self._enzyme = enzyme
        self._cache: List[ReconciliationResult] = []
        self._cache_turn: int = 0
        self._last_reconciled_ts: Optional[str] = None

    def reconcile_prefetch(self, query: str, turn: int = 0, last_reconciled_ts: Optional[str] = None) -> List[ReconciliationResult]:
        """Run reconciliation for prefetch injection.

        Phase 1 always runs. Phase 2 only for high-importance claims
        or when the query directly relates to a claim.
        Returns conflicts and tensions formatted for context injection.
        """
        # Skip if cache is fresh (same turn)
        if self._cache and self._cache_turn == turn:
            return self._cache

        results: List[ReconciliationResult] = []
        
        # Optimization: Use change-tracking instead of full scan
        effective_ts = last_reconciled_ts or self._last_reconciled_ts

        if effective_ts:
            recent_claims = self._ladybug.get_changes_since(effective_ts, limit=30)
            if not recent_claims:
                self._cache = results
                self._cache_turn = turn
                return results
        else:
            recent_claims = self._ladybug.get_recent_claims(limit=15)

        if not recent_claims:
            self._cache = results
            self._cache_turn = turn
            return results

        # If enzyme is not available, skip reconciliation entirely
        if not self._enzyme or not self._enzyme.available:
            self._cache = results
            self._cache_turn = turn
            return results

        # Phase 1: Operational — keyword extraction + petri entity matching
        petri_data = self._enzyme.petri(query=query or None, top=15)
        petri_entities = set()
        petri_output_text = ""
        
        if "entities" in petri_data:
            # JSON petri output — extract entity names directly
            for entity in petri_data["entities"]:
                name = entity.get("name", "") or str(entity).lower()
                if name:
                    petri_entities.add(name.lower().split(":")[0].strip())
        elif "output" in petri_data:
            petri_output_text = str(petri_data["output"])
            # Text petri output — parse line by line
            for line in petri_output_text.split("\n"):
                line = line.strip()
                if line and not line.startswith(("-", "*", "#")):
                    petri_entities.add(line.lower().split(":")[0].strip())
        elif "results" in petri_data:
            # Fallback: catalyze-style results used as petri substitute
            for r in petri_data["results"][:10]:
                path = r.get("file_path", "")
                if path:
                    petri_entities.add(path.lower().split("/")[-1].split(".")[0])

        for claim_entry in recent_claims:
            content = claim_entry["content"]
            importance = claim_entry.get("importance", 5)

            # Extract operational keywords from the claim
            claim_keywords = self._extract_keywords(content)
            overlap = claim_keywords & petri_entities

            # Check if this claim warrants deep cognitive verification
            # High importance claims (or query-relevant ones) bypass the petri-overlap requirement
            is_high_importance = (importance >= 7 or self._query_matches_claim(query, content))

            if is_high_importance:
                # Phase 2: Cognitive — deep check
                # CAPPED: only run catalyze subprocess for top 3 claims to bound
                # latency (each catalyze spawns a subprocess, ~50-200ms)
                phase2_budget = 3
                phase2_used = sum(
                    1 for r in results
                    if r.status in (ReconciliationStatus.CONFLICT,
                                    ReconciliationStatus.TENSION,
                                    ReconciliationStatus.VERIFIED)
                    and r.evidence and "Operational match" not in r.evidence
                )
                
                if phase2_used < phase2_budget:
                    deep_result = self._phase2_check(claim_entry, query)
                    results.append(deep_result)
                else:
                    # Budget exhausted, fall back to operational check if possible
                    if overlap:
                        results.append(ReconciliationResult(
                            claim=content,
                            status=ReconciliationStatus.VERIFIED,
                            evidence=f"Operational match: {', '.join(overlap)} (Phase 2 budget exceeded)",
                            importance=importance,
                            source_entries=[claim_entry["base_id"]],
                        ))
                    else:
                        results.append(ReconciliationResult(
                            claim=content,
                            status=ReconciliationStatus.UNVERIFIED,
                            evidence="High importance but Phase 2 budget exceeded",
                            importance=importance,
                            source_entries=[claim_entry["base_id"]],
                        ))
            elif overlap:
                # Operational match found, assume verified unless contradicted later
                results.append(ReconciliationResult(
                    claim=content,
                    status=ReconciliationStatus.VERIFIED,
                    evidence=f"Operational match: {', '.join(overlap)}",
                    importance=importance,
                    source_entries=[claim_entry["base_id"]],
                ))
            else:
                # Claim topics not in current petri — could be new or irrelevant
                results.append(ReconciliationResult(
                    claim=content,
                    status=ReconciliationStatus.UNVERIFIED,
                    evidence="No operational overlap with current vault entities",
                    importance=importance,
                    source_entries=[claim_entry["base_id"]],
                ))

        # Update the internal tracking timestamp to the latest entry processed
        # This ensures next run only picks up things AFTER this batch.
        if recent_claims:
            # We take the max updated_at to advance our cursor correctly
            self._last_reconciled_ts = max(c["updated_at"] for c in recent_claims)

        # Only cache conflicts and tensions — these are what we inject
        self._cache = [r for r in results if r.status in (
            ReconciliationStatus.TENSION,
            ReconciliationStatus.CONFLICT,
        )]
        self._cache_turn = turn
        return self._cache

    def verify_claim(self, claim: str) -> ReconciliationResult:
        """Explicitly verify a claim against Enzyme workspace knowledge.

        Used by the memory_verify tool. Always runs Phase 2 (deep check).
        """
        # Find the best matching Ladybug entry
        matches = self._ladybug.search(claim, limit=3)
        claim_entry = matches[0] if matches else None

        if not self._enzyme or not self._enzyme.available:
            # Enzyme not available — can't verify, return unverified
            return ReconciliationResult(
                claim=claim,
                status=ReconciliationStatus.UNVERIFIED,
                evidence="Enzyme not available — workspace verification skipped",
            )

        if claim_entry:
            return self._phase2_check(claim_entry, claim)

        # No Ladybug entry — check Enzyme directly
        catalyze_result = self._enzyme.catalyze(claim, limit=5, register="reference")
        if "error" in catalyze_result:
            return ReconciliationResult(
                claim=claim,
                status=ReconciliationStatus.UNVERIFIED,
                evidence=f"Enzyme query failed: {catalyze_result['error']}",
            )

        # If Enzyme returns high-similarity results, the claim IS verified
        # by workspace knowledge even if Ladybug has no matching entry
        results = catalyze_result.get("results", [])
        if results and results[0].get("similarity", 0) >= 0.6:
            return ReconciliationResult(
                claim=claim,
                status=ReconciliationStatus.VERIFIED,
                evidence=self._format_catalyze_evidence(catalyze_result),
            )

        return ReconciliationResult(
            claim=claim,
            status=ReconciliationStatus.UNVERIFIED,
            evidence=self._format_catalyze_evidence(catalyze_result),
        )

    def _phase2_check(self, claim_entry: Dict[str, Any],
                      query: str) -> ReconciliationResult:
        """Deep verification using enzyme catalyze."""
        content = claim_entry["content"]
        importance = claim_entry.get("importance", 5)

        catalyze_result = self._enzyme.catalyze(
            content, limit=5, register="reference"
        )

        evidence = self._format_catalyze_evidence(catalyze_result)

        # Simple heuristic conflict detection:
        # If catalyze returns results with high similarity that use negation
        # or contradictory framing relative to the claim
        has_contradiction = self._detect_contradiction(content, catalyze_result)
        has_tension = self._detect_tension(content, catalyze_result)

        if has_contradiction:
            status = ReconciliationStatus.CONFLICT
        elif has_tension:
            status = ReconciliationStatus.TENSION
        elif evidence:
            status = ReconciliationStatus.VERIFIED
        else:
            status = ReconciliationStatus.UNVERIFIED

        return ReconciliationResult(
            claim=content,
            status=status,
            evidence=evidence,
            importance=importance,
            source_entries=[claim_entry["base_id"]],
        )

    def format_for_context(self, results: List[ReconciliationResult]) -> str:
        """Format reconciliation results for context injection."""
        if not results:
            return ""

        lines = ["## Memory Reconciliation Alerts", ""]
        for r in results:
            icon = {"conflict": "X", "tension": "!"}.get(r.status.value, "?")
            lines.append(f"[{icon}] {r.status.value.upper()}: {r.claim}")
            if r.evidence:
                lines.append(f"    Evidence: {r.evidence}")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _extract_keywords(text: str) -> set:
        """Extract lowercase keywords, filtering stop words."""
        stop = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been",
            "have", "has", "had", "do", "does", "did", "will", "would",
            "could", "should", "may", "might", "shall", "can", "need",
            "it", "its", "this", "that", "these", "those", "i", "you",
            "he", "she", "we", "they", "me", "him", "her", "us", "them",
            "my", "your", "his", "her", "our", "their", "and", "or",
            "but", "if", "of", "at", "by", "for", "with", "about", "to",
            "from", "in", "on", "not", "no", "so", "as", "than", "then",
        }
        words = re.findall(r"[a-zA-Z_]{3,}", text.lower())
        return {w for w in words if w not in stop}

    @staticmethod
    def _query_matches_claim(query: str, claim: str) -> bool:
        """Check if a query is semantically related to a claim (keyword overlap)."""
        q_words = set(re.findall(r"[a-zA-Z_]{3,}", query.lower()))
        c_words = set(re.findall(r"[a-zA-Z_]{3,}", claim.lower()))
        overlap = q_words & c_words
        return len(overlap) >= 2

    @staticmethod
    def _format_catalyze_evidence(result: Dict[str, Any]) -> str:
        """Extract readable evidence from a catalyze result."""
        if "error" in result:
            return f"Enzyme error: {result['error']}"

        # Handle JSON catalyze output
        if "results" in result:
            excerpts = []
            for r in result["results"][:3]:
                path = r.get("file_path", "?")
                content = r.get("content", "")[:120]
                sim = r.get("similarity", 0)
                excerpts.append(f"[{path}] ({sim:.2f}) {content}")
            return " | ".join(excerpts)

        # Handle text catalyze output
        output = result.get("output", "")
        if isinstance(output, str):
            return output[:300]
        return str(result)[:300]

    @staticmethod
    def _detect_contradiction(claim: str, catalyze_result: Dict[str, Any]) -> bool:
        """Heuristic: detect if catalyze results directly contradict the claim.

        Looks for negation patterns in high-similarity results.
        This is intentionally conservative - false negatives are preferred
        over false positives.
        """
        results = catalyze_result.get("results", [])
        if not results:
            return False

        negation_patterns = [
            "not ", "don't ", "doesn't ", "isn't ", "aren't ",
            "wasn't ", "weren't ", "never ", "no longer ",
            "instead of ", "replaced by ", "deprecated ",
        ]

        claim_lower = claim.lower()
        for r in results[:2]:  # Only check top 2 (highest similarity)
            sim = r.get("similarity", 0)
            if sim < 0.6:  # Only trust high-similarity results
                continue
            content = r.get("content", "").lower()
            # Check if a negated version of a claim keyword appears in evidence
            for word in claim_lower.split():
                if len(word) < 4:
                    continue
                for neg in negation_patterns:
                    if neg + word in content:
                        return True
        return False

    @staticmethod
    def _detect_tension(claim: str, catalyze_result: Dict[str, Any]) -> bool:
        """Heuristic: detect if catalyze results suggest tension with the claim.

        Looks for qualifying language in results that partially overlap.
        """
        results = catalyze_result.get("results", [])
        if not results:
            return False

        tension_patterns = [
            "however", "but ", "although", "except", "unless",
            "previously", "used to", "changed to", "updated ",
            "migrated ", "moved to",
        ]

        for r in results[:3]:
            content = r.get("content", "").lower()
            for pattern in tension_patterns:
                if pattern in content:
                    return True
        return False
