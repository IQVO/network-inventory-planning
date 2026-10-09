import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import type * as OpenApiPlugin from 'docusaurus-plugin-openapi-docs';

const config: Config = {
  title: 'Network Inventory Planning',
  tagline:
    'Inter-warehouse transfer recommendations from fail-closed local facts, and the operator-approved transfer saga from origin reservation to destination stow. A WES-tier Core context.',
  favicon: 'img/favicon.svg',

  future: {
    v4: true,
    faster: true,
  },

  url: 'https://iqvo.github.io',
  baseUrl: '/network-inventory-planning/',

  organizationName: 'IQVO',
  projectName: 'network-inventory-planning',
  deploymentBranch: 'gh-pages',
  trailingSlash: false,

  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  markdown: {
    mermaid: true,
    hooks: {
      onBrokenMarkdownLinks: 'throw',
    },
  },

  presets: [
    [
      'classic',
      {
        docs: {
          // ONE docs instance: the ADRs already live inside the content root
          // (docs/docs/adr/*.md) and are served in place under /docs/adr.
          sidebarPath: './sidebars.ts',
          // Keep the 0001- prefix in ADR ids/URLs so they match the file names.
          numberPrefixParser: false,
          editUrl:
            'https://github.com/IQVO/network-inventory-planning/tree/main/docs/',
          docItemComponent: '@theme/ApiItem',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  plugins: [
    [
      'docusaurus-plugin-openapi-docs',
      {
        id: 'openapi',
        docsPluginId: 'classic',
        config: {
          'network-inventory-planning': {
            // The single source of truth: the same Spectral-linted spec the
            // service ships and CI gates on. Never hand-transcribed here.
            specPath: '../apis/openapi.yaml',
            outputDir: 'docs/api-reference/rest',
            sidebarOptions: {
              groupPathsBy: 'tag',
              categoryLinkSource: 'tag',
            },
            hideSendButton: true,
          } satisfies OpenApiPlugin.Options,
        },
      },
    ],
  ],

  themes: ['docusaurus-theme-openapi-docs', '@docusaurus/theme-mermaid'],

  themeConfig: {
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'Network Inventory Planning',
      logo: {
        alt: 'Network Inventory Planning',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          to: '/docs/api-reference/overview',
          label: 'API Reference',
          position: 'left',
        },
        {
          to: '/docs/adr/about',
          label: 'ADRs',
          position: 'left',
        },
        {
          href: 'https://github.com/IQVO/network-inventory-planning',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Documentation',
          items: [
            {label: 'Introduction', to: '/docs/overview/introduction'},
            {label: 'Quickstart', to: '/docs/overview/quickstart'},
            {label: 'Runbook', to: '/docs/operations/runbook'},
            {label: 'API Reference', to: '/docs/api-reference/overview'},
            {label: 'Architecture decisions', to: '/docs/adr/about'},
          ],
        },
        {
          title: 'Neighbouring contexts',
          items: [
            {label: 'inventory-storage', href: 'https://github.com/IQVO/inventory-storage'},
            {label: 'wes-work-planning', href: 'https://github.com/IQVO/wes-work-planning'},
            {label: 'fulfillment-execution', href: 'https://github.com/IQVO/fulfillment-execution'},
            {label: 'facility-layout', href: 'https://github.com/IQVO/facility-layout'},
            {label: 'order-management', href: 'https://github.com/IQVO/order-management'},
            {label: 'warehouse-planning', href: 'https://github.com/IQVO/warehouse-planning'},
          ],
        },
        {
          title: 'Source',
          items: [
            {label: 'GitHub repository', href: 'https://github.com/IQVO/network-inventory-planning'},
            {label: 'OpenAPI spec', href: 'https://raw.githubusercontent.com/IQVO/network-inventory-planning/main/apis/openapi.yaml'},
            {label: 'AsyncAPI spec', href: 'https://raw.githubusercontent.com/IQVO/network-inventory-planning/main/apis/asyncapi.yaml'},
          ],
        },
      ],
      copyright: `Network Inventory Planning — a warehouse-systems bounded context. Built ${new Date().getFullYear()}.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'go', 'json', 'yaml', 'sql'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
