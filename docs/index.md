---
layout: home

hero:
  name: Membrane
  text: Selective Learning and Memory for Agentic Systems
  tagline: A structured memory substrate that lets AI agents remember, learn, and forget -- just like biological cognition.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/getting-started
    - theme: alt
      text: View on GitHub
      link: https://github.com/GustyCube/membrane

features:
  - title: Five Memory Types
    details: Episodic, working, semantic, competence, and plan graph memories mirror how humans store and recall different kinds of knowledge.
  - title: Trust-Aware Retrieval
    details: Every query carries a trust context that gates access by sensitivity level and scope, keeping private data private.
  - title: Automatic Consolidation
    details: Raw experiences are periodically distilled into stable facts, learned procedures, and reusable plans -- no user intervention required.
  - title: Decay and Reinforcement
    details: Salience decays over time using configurable curves. Frequently accessed memories are reinforced; stale ones fade away.
  - title: Revisable Knowledge
    details: Semantic facts support supersede, fork, retract, and merge operations with full audit trails.
  - title: gRPC API
    details: A 13-method gRPC service exposes ingestion, retrieval, revision, reinforcement, and metrics to any language.
---

## Quick Start

```bash
# Build from source
git clone https://github.com/GustyCube/membrane.git
cd membrane
make build

# Start the daemon
./bin/membraned -db membrane.db -addr :9090
```

See the [Getting Started](/guide/getting-started) guide for a complete walkthrough.
