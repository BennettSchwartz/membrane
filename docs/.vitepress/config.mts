import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'Membrane',
  description: 'A selective learning and memory substrate for agentic systems',
  base: '/',
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'API', link: '/api/grpc' },
      { text: 'Reference', link: '/reference/schemas' },
    ],
    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Core Concepts', link: '/guide/concepts' },
          { text: 'Memory Types', link: '/guide/memory-types' },
          { text: 'Retrieval', link: '/guide/retrieval' },
          { text: 'Consolidation', link: '/guide/consolidation' },
          { text: 'Security', link: '/guide/security' },
        ]
      },
      {
        text: 'API Reference',
        items: [
          { text: 'gRPC API', link: '/api/grpc' },
        ]
      },
      {
        text: 'Reference',
        items: [
          { text: 'Schemas', link: '/reference/schemas' },
          { text: 'Configuration', link: '/reference/configuration' },
        ]
      }
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/GustyCube/membrane' }
    ]
  }
})
