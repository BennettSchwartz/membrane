import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Membrane',
  description: 'A selective learning and memory substrate for agentic systems',
  base: '/',
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API', link: '/api/grpc' },
      { text: 'Internals', link: '/internals/architecture' },
      { text: 'Reference', link: '/reference/schemas' },
    ],
    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Core Concepts', link: '/guide/concepts' },
          { text: 'Memory Types', link: '/guide/memory-types' },
          { text: 'Ingestion', link: '/guide/ingestion' },
          { text: 'Retrieval', link: '/guide/retrieval' },
          { text: 'Revision Operations', link: '/guide/revision' },
          { text: 'Decay & Reinforcement', link: '/guide/decay' },
          { text: 'Consolidation', link: '/guide/consolidation' },
          { text: 'Security', link: '/guide/security' },
          { text: 'Deployment', link: '/guide/deployment' },
          { text: 'Examples & Recipes', link: '/guide/examples' },
          { text: 'Troubleshooting', link: '/guide/troubleshooting' },
        ]
      },
      {
        text: 'API Reference',
        items: [
          { text: 'gRPC API', link: '/api/grpc' },
          { text: 'Go Packages', link: '/api/go-packages' },
        ]
      },
      {
        text: 'Internals',
        items: [
          { text: 'Architecture', link: '/internals/architecture' },
          { text: 'Storage Layer', link: '/internals/storage' },
          { text: 'Metrics & Observability', link: '/internals/metrics' },
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Schemas', link: '/reference/schemas' },
          { text: 'Configuration', link: '/reference/configuration' },
          { text: 'RFC', link: '/reference/rfc' },
        ]
      }
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/GustyCube/membrane' }
    ]
  }
})
