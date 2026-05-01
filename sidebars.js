const sidebars = {
  docs: [
    "index",
    {
      type: "category",
      label: "Get Started",
      collapsed: false,
      items: ["introduction", "quickstart", "architecture"],
    },
    {
      type: "category",
      label: "Core Concepts",
      collapsed: false,
      items: [
        "concepts/memory-types",
        "concepts/trust-and-sensitivity",
        "concepts/decay-and-consolidation",
        "concepts/revision-operations",
        "concepts/competence-learning",
        "concepts/plan-graphs",
      ],
    },
    {
      type: "category",
      label: "Guides",
      collapsed: false,
      items: [
        "guides/go-library",
        "guides/llm-integration",
        "guides/deployment",
        "guides/configuration",
        "guides/security",
        "guides/observability",
      ],
    },
    {
      type: "category",
      label: "Client SDKs",
      collapsed: false,
      items: ["sdks/typescript", "sdks/python", "sdks/openclaw-plugin"],
    },
    {
      type: "category",
      label: "gRPC API",
      collapsed: false,
      items: [
        "api/overview",
        "api/ingestion",
        "api/retrieval",
        "api/revision",
        "api/metrics",
      ],
    },
    {
      type: "category",
      label: "Go Library API",
      collapsed: false,
      items: [
        "api/go/membrane",
        "api/go/schema",
        "api/go/ingestion",
        "api/go/retrieval",
        "api/go/revision",
      ],
    },
  ],
};

module.exports = sidebars;
