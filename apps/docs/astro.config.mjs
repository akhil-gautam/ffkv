import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { unified } from '@astrojs/markdown-remark';

const base = `/${(process.env.DOCS_BASE ?? '').replace(/^\/+|\/+$/g, '')}`.replace(/\/?$/, '/');

// Markdown URLs are authored from the documentation root. Keep the same content
// deployable at a domain root or project subpath without rewriting every guide.
function documentationLinks() {
  return (tree) => {
    const visit = (node) => {
      for (const key of ['href', 'src']) {
        const value = node.properties?.[key];
        if (typeof value === 'string' && value.startsWith('/') && !value.startsWith('//')) {
          node.properties[key] = `${base.replace(/\/$/, '')}${value}`;
        }
      }
      node.children?.forEach(visit);
    };
    visit(tree);
  };
}

export default defineConfig({
  site: process.env.DOCS_SITE ?? 'http://localhost:4321',
  base,
  trailingSlash: 'always',
  server: { host: '127.0.0.1', port: 4321 },
  vite: { server: { strictPort: true }, preview: { strictPort: true } },
  markdown: { processor: unified({ rehypePlugins: [documentationLinks] }) },
  integrations: [
    starlight({
      title: 'ffkv',
      description: 'The ffkv handbook. Build, use and operate freaking fast KV with precise, CLI-testable contracts.',
      favicon: '/favicon.svg',
      defaultLocale: 'root',
      locales: { root: { label: 'English', lang: 'en' } },
      customCss: [
        '@fontsource-variable/geist',
        '@fontsource-variable/geist-mono',
        '@fontsource/instrument-serif/400.css',
        '@fontsource/instrument-serif/400-italic.css',
        './src/styles/docs.css',
      ],
      components: {
        SiteTitle: './src/components/SiteTitle.astro',
        Footer: './src/components/Footer.astro',
        Search: './src/components/Search.astro',
        ThemeSelect: './src/components/ThemeSelect.astro',
      },
      sidebar: [
        { label: 'Start here', items: [
          { label: 'Introduction', link: '/' },
          { label: 'Installation', slug: 'getting-started/installation' },
          { label: 'Quickstart', slug: 'getting-started/quickstart' },
          { label: 'Browser playground', slug: 'playground' },
          { label: 'Native CLI', slug: 'getting-started/cli' },
          { label: 'MCP integration', slug: 'guides/mcp' },
          { label: 'Management console', slug: 'getting-started/console' },
        ] },
        { label: 'Understand ffkv', items: [
          { label: 'Architecture', slug: 'concepts/architecture' },
          { label: 'Compatibility', slug: 'concepts/compatibility' },
        ] },
        { label: 'Data types', items: [{ autogenerate: { directory: 'data-types' } }], collapsed: true },
        { label: 'Build with ffkv', items: [{ autogenerate: { directory: 'guides' } }], collapsed: true },
        { label: 'Operate', items: [{ autogenerate: { directory: 'operations' } }], collapsed: true },
        { label: 'Deploy', items: [{ autogenerate: { directory: 'deployment' } }], collapsed: true },
        { label: 'Reference', items: [
          { label: 'Command explorer', slug: 'reference/commands' },
          { label: 'ffkv extensions', slug: 'reference/extensions' },
          { label: 'Server configuration', slug: 'reference/configuration' },
          { label: 'Management API', slug: 'reference/management-api' },
          { label: 'Limits and guarantees', slug: 'reference/limits' },
        ] },
        { label: 'Contribute', items: [{ autogenerate: { directory: 'contributing' } }], collapsed: true },
      ],
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      pagination: true,
      lastUpdated: false,
      credits: false,
    }),
  ],
});
