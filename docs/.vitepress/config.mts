import { defineConfig } from 'vitepress';

export default defineConfig({
  base: '/pg_liquid/',
  title: 'pg_liquid',
  description: 'LIquid-style graph and ontology querying for PostgreSQL',
  srcDir: '.',
  cleanUrls: true,
  lastUpdated: true,
  themeConfig: {
    logo: '/logo.svg',
    siteTitle: 'pg_liquid',
    nav: [
      { text: 'Guide', link: '/getting-started/install' },
      { text: 'Reference', link: '/reference/api' },
      { text: 'GitHub', link: 'https://github.com/michael-golfi/pg_liquid' },
      { text: 'PGXN', link: 'https://pgxn.org/dist/pg_liquid/' },
    ],
    sidebar: [
      {
        text: 'Start Here',
        items: [
          { text: 'What pg_liquid Is', link: '/index' },
          { text: 'Install', link: '/getting-started/install' },
          { text: 'Create Your First Graph', link: '/getting-started/first-graph' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Query a Graph', link: '/guides/query-graphs' },
          { text: 'Model Compounds', link: '/guides/compounds' },
          { text: 'Build an Ontology', link: '/guides/ontologies' },
          { text: 'Add Row Normalizers', link: '/guides/normalizers' },
          { text: 'Use CLS and Principal Scopes', link: '/guides/security' },
          { text: 'Upgrade and Operate', link: '/guides/operations' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'SQL API', link: '/reference/api' },
          { text: 'LIquid Language Surface', link: '/reference/language' },
          { text: 'Data Model', link: '/reference/data-model' },
          { text: 'Execution Model', link: '/reference/execution' },
          { text: 'Storage Layout', link: '/reference/storage' },
          { text: 'Security Model', link: '/reference/security-model' },
          { text: 'Testing and Compatibility', link: '/reference/testing' },
          { text: 'Roadmap', link: '/reference/roadmap' },
        ],
      },
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/michael-golfi/pg_liquid' },
    ],
    search: {
      provider: 'local',
    },
    footer: {
      message: 'Released under the MIT License.',
      copyright: 'Copyright © Michael Golfi',
    },
  },
});
