// @ts-check

const config = {
  title: "Membrane",
  tagline: "A selective learning and memory substrate for LLM and agentic systems.",
  url: process.env.DOCS_SITE_URL ?? "https://membrane.gustycube.com",
  baseUrl: "/",
  organizationName: "BennettSchwartz",
  projectName: "membrane",
  favicon: "img/favicon.svg",
  trailingSlash: false,
  onBrokenLinks: "warn",

  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "warn",
    },
  },

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      {
        docs: {
          path: "docs",
          routeBasePath: "/",
          sidebarPath: "./sidebars.js",
          editUrl: "https://github.com/BennettSchwartz/membrane/edit/master/docs/",
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
        sitemap: {
          changefreq: "weekly",
          priority: 0.5,
        },
      },
    ],
  ],

  themeConfig: {
    navbar: {
      title: "Membrane",
      logo: {
        alt: "Membrane logo",
        src: "img/logo-mark.svg",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Docs",
        },
        {
          href: "https://github.com/BennettSchwartz/membrane",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            { label: "Quickstart", to: "/quickstart" },
            { label: "API Reference", to: "/api/overview" },
            { label: "TypeScript SDK", to: "/sdks/typescript" },
            { label: "Python SDK", to: "/sdks/python" },
          ],
        },
        {
          title: "Community",
          items: [
            { label: "GitHub", href: "https://github.com/BennettSchwartz/membrane" },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} BennettSchwartz.`,
    },
    prism: {
      additionalLanguages: ["bash", "go", "json", "python", "typescript", "yaml"],
    },
    colorMode: {
      defaultMode: "dark",
      respectPrefersColorScheme: true,
    },
  },
};

module.exports = config;
