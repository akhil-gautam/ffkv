import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { parse } from 'parse5';

const root = fileURLToPath(new URL('../', import.meta.url));
const dist = path.join(root, 'dist');
const base = `/${(process.env.DOCS_BASE ?? '').replace(/^\/+|\/+$/g, '')}`.replace(/\/?$/, '/');
const origin = new URL(process.env.DOCS_SITE ?? 'http://localhost:4321').origin;
const files = new Set();
async function collect(directory, prefix = '') {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const relative = path.posix.join(prefix, entry.name);
    if (entry.isDirectory()) await collect(path.join(directory, entry.name), relative);
    else files.add(relative);
  }
}
await collect(dist);
const errors = [];
const pages = new Map();
const text = (node) => node.nodeName === '#text' ? node.value : (node.childNodes ?? []).map(text).join('');
function walk(node, visitor) {
  visitor(node);
  for (const child of node.childNodes ?? []) walk(child, visitor);
}
for (const file of [...files].filter((file) => file.endsWith('.html'))) {
  const source = await readFile(path.join(dist, file), 'utf8');
  const tree = parse(source);
  const ids = new Set();
  const links = [];
  const headings = [];
  let title = '';
  let description = '';
  let canonical = '';
  let language = '';
  walk(tree, (node) => {
    if (!node.tagName) return;
    const attributes = Object.fromEntries(node.attrs.map(({ name, value }) => [name, value]));
    if (attributes.id) {
      if (ids.has(attributes.id)) errors.push(`${file}: duplicate id ${attributes.id}`);
      ids.add(attributes.id);
    }
    if (node.tagName === 'h1') headings.push(text(node).trim());
    if (node.tagName === 'title') title = text(node).trim();
    if (node.tagName === 'html') language = attributes.lang;
    if (node.tagName === 'meta' && attributes.name === 'description') description = attributes.content;
    if (node.tagName === 'link' && attributes.rel === 'canonical') {
      canonical = attributes.href;
      return;
    }
    if (node.tagName === 'img' && !Object.hasOwn(attributes, 'alt')) errors.push(`${file}: image lacks alt text`);
    for (const attribute of ['href', 'src']) {
      if (attributes[attribute]) links.push(attributes[attribute]);
    }
  });
  if (headings.length !== 1) errors.push(`${file}: expected one page heading; received ${headings.length}`);
  if (!title || !description || !language || !canonical) errors.push(`${file}: missing title, description, language or canonical URL`);
  const route = file === 'index.html' ? '' : file.endsWith('/index.html') ? file.slice(0, -'index.html'.length) : file;
  const url = new URL(base + route, origin);
  if (canonical && new URL(canonical).origin !== origin) errors.push(`${file}: canonical origin differs from DOCS_SITE`);
  if (canonical && file !== '404.html' && new URL(canonical).pathname !== url.pathname) errors.push(`${file}: canonical path differs from the built route`);
  pages.set(file, { ids, links, headings, url });
}
let checkedLinks = 0;
for (const [file, page] of pages) {
  for (const value of new Set(page.links)) {
    let url;
    try { url = new URL(value, page.url); }
    catch { errors.push(`${file}: malformed URL ${value}`); continue; }
    if (!['http:', 'https:'].includes(url.protocol) || url.origin !== origin) continue;
    if (!url.pathname.startsWith(base)) {
      errors.push(`${file}: internal URL escapes DOCS_BASE: ${value}`);
      continue;
    }
    let relative;
    try { relative = decodeURIComponent(url.pathname.slice(base.length)); }
    catch { errors.push(`${file}: malformed URL encoding ${value}`); continue; }
    const candidates = [relative, path.posix.join(relative, 'index.html')];
    const target = candidates.find((candidate) => files.has(candidate));
    if (!target) {
      errors.push(`${file}: broken internal link ${value}`);
      continue;
    }
    checkedLinks += 1;
    if (url.hash && pages.has(target)) {
      const fragment = decodeURIComponent(url.hash.slice(1));
      if (fragment && !pages.get(target).ids.has(fragment)) errors.push(`${file}: missing anchor ${value}`);
    }
  }
}
const catalog = JSON.parse(await readFile(new URL('../../../src/command_catalog.json', import.meta.url), 'utf8'));
for (const command of catalog.commands) {
  const slug = command.name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  const destination = `reference/commands/${slug}/index.html`;
  if (!pages.has(destination)) errors.push(`Missing command page: ${command.name}`);
  else if (pages.get(destination).headings[0] !== command.name) errors.push(`${destination}: command heading does not identify ${command.name}`);
}
for (const route of [
  'getting-started/installation', 'getting-started/quickstart', 'playground', 'getting-started/cli', 'getting-started/console',
  'concepts/architecture', 'concepts/compatibility', 'data-types', 'guides/native-jobs', 'guides/list-queues',
  'guides/sidekiq', 'guides/transactions', 'guides/lua-functions', 'guides/pubsub', 'guides/modules', 'guides/search',
  'operations/persistence', 'operations/hybrid-storage', 'operations/backup-restore', 'operations/security',
  'operations/memory', 'operations/observability', 'operations/replication', 'operations/clustering',
  'operations/migration', 'operations/troubleshooting', 'deployment/docker', 'deployment/kubernetes',
  'deployment/documentation', 'reference/commands', 'reference/extensions', 'reference/configuration',
  'reference/management-api', 'reference/limits', 'contributing/development', 'contributing/testing', 'contributing/benchmarks',
]) {
  if (!pages.has(`${route}/index.html`)) errors.push(`Missing documentation route: ${route}`);
}
if (!files.has('pagefind/pagefind.js')) errors.push('Missing production search index');
if (!files.has('playground/ffkv.wasm')) errors.push('Missing playground engine (playground/ffkv.wasm)');
if (errors.length) {
  console.error(errors.join('\n'));
  process.exitCode = 1;
} else {
  assert.equal(catalog.commands.length, 486, 'Update the pinned command-catalog audit when changing the baseline');
  console.log(`Docs integrity passed: ${pages.size} HTML pages, ${catalog.commands.length} canonical command pages, ${checkedLinks} local links/assets/anchors, production search index.`);
}
