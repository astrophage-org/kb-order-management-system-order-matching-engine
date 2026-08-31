# 🤖 AGENTS.md — Operational Contract for AI Agents

This repository is an **OpenKB Knowledge Base** for **order-management-system/order-matching-engine** maintained by Project Astrophage.

## 📂 Vault Structure
- `index.md`: Master architecture overview and navigation registry.
- `summaries/`: Technical summaries (e.g. `api-spec.md`, `db-schema.md`, `core-logic.md`).
- `concepts/`: High-level domain concepts and system data flows.
- `entities/`: Microservices, database models, and integrations.
- `decisions/`: Architecture Decision Records (ADRs).
- `log.md`: Chronological changelog and agent audit trail.
- `.astrophage/brief.md`: Compact architecture digest for prompt injection (<4,000 chars).

## ⚖️ Invariants & Rules for AI Agents
1. **Wikilink Syntax**: Use standard Obsidian-compatible `[[path/slug|Title]]` or `[[slug]]` for all internal cross-references. For cross-repo links, use `[[ap:target-repo/path|Title]]`.
2. **Zero Broken Links**: Every `[[wikilink]]` must resolve to an existing file in this vault or registered cross-KB repository.
3. **Append to log.md**: Whenever modifying or adding a page, append a dated entry to `log.md`.
4. **Preserve Anchors**: Never delete `<!-- anchor: ... -->` comments in summaries or entity pages.
