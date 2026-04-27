# Unified Dual-Core Memory Architecture (UDMA)

The **UDMA Framework** is a high-fidelity memory management system designed for autonomous AI agents. It implements a "Dual-Core" cognitive architecture that resolves the inherent tension between **Episodic Truth** (what is happening now) and **Workspace Truth** (what is documented in the environment).

## The Problem: Contextual Drift

Standard LLM agents rely on a single, flat context window. As a session progresses, the agent's "truth" drifts:
1. **Hallucination**: The agent forgets a previously established constraint.
2. **Stale Knowledge**: The agent follows an outdated instruction that has since been superseded in the codebase.
3. **Semantic Compression**: Important nuances are lost in summarized context.

## The Solution: The Dual-Core Architecture

UDMA implements a tiered, bifurcated memory system:

### 1. Ladybug (Episodic Memory)
The "Dynamic Truth" core. It captures the flow of the current interaction—recent decisions, shifting preferences, and session facts. It is high-speed and reactive.

### 2. Enzyme (Workspace Memory)
The "Static Truth" core. It bridges the agent to the persistent environment (codebases, documentation, vector stores). It represents the "rock" of accumulated, verified knowledge.

### 3. The Semantic Bridge (Reconciliation)
This is the heart of UDMA. A background engine continuously performs **Semantic Reconciliation** between Ladybug and Enzyme. It uses a two-phase approach:

* **Phase 1 (Operational)**: Fast, deterministic keyword and entity matching.
* **Phase 2 (Cognitive)**: Deep semantic comparison using high-fidelity LLM queries to detect subtle tensions or direct contradictions.

When a contradiction is found, the engine generates a **Reconciliation Alert**, which is injected into the agent's context, forcing the agent to confront the conflict and correct its path.

## Installation

### Via Pip
```bash
pip install git+https://github.com/yourname/dualcore-memory.git
```

### Via Plugin Directory (Hermes Agent)
```bash
git clone https://github.com/yourname/dualcore-memory ~/.hermes/plugins/dualcore
```

## Extending the Framework

UDMA is designed to be extensible. By implementing the `EnzymeBridge` abstract base class, you can connect the reconciliation engine to any source of truth:

```python
from dualcore.enzyme import EnzymeBridge

class MyCustomEnzyme(EnzymeBridge):
    # Implement petri(), catalyze(), etc.
    ...
```

## License
MIT
