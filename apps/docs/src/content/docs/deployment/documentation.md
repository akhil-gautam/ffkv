---
title: Documentation site
description: Run, maintain and deploy the ffkv Astro handbook as static files, independently of the KV server and management console.
sidebar:
  order: 3
---

The handbook is a separate **Astro + Starlight** application in `apps/docs`. It builds to static HTML, CSS, JavaScript and a local Pagefind search index. It does not require a running ffkv instance, a Redis server, the React console, a hosted search service or a database.

## Run locally

Use Node **22.23.4 or newer** and npm. From the repository root:

```sh
npm --prefix apps/docs ci
npm --prefix apps/docs run dev
```

Open **http://localhost:4321**. The development server binds loopback and refuses an occupied port instead of silently using a different one. Stop only this process with Ctrl-C. It does not manage any KV processes or data directories.

The pinned current Astro release requires this Node patch level. If your global
Node is older, npm can provide an isolated runtime without replacing it:

```sh
npx --yes --package node@22.23.4 -- npm --prefix apps/docs ci
npx --yes --package node@22.23.4 -- npm --prefix apps/docs run dev
```

Use the same `npx --yes --package node@22.23.4 -- npm` prefix for check, build and
preview commands while using that runtime. The isolated runtime is stored in npm's
package cache, not installed as the system `node`.

Development search depends on a production search index; use the production preview below to verify search, not an empty development index.

## Check and build

```sh
npm --prefix apps/docs run check
npm --prefix apps/docs run build
npm --prefix apps/docs run preview
```

`check` runs Astro's content and TypeScript diagnostics. `build` creates `apps/docs/dist`, builds Pagefind, then audits **every generated HTML page** for page metadata, a single page heading, duplicate IDs, local links/assets and fragment destinations. It also checks every canonical command route and the production search index. A broken reference fails the build.

The preview serves the built site on loopback. It is a local smoke server, not a production process manager. Inspect the URL printed by Astro. The developer preview and production site are public documentation; neither presents a console token prompt.

Search uses Pagefind's documented main-thread execution mode with a local index;
queries do not go to a hosted search service. This avoids the pinned runtime's
root-relative URL failure inside blob workers without patching vendor files.

Toggle search with the header button or **⌘K / Ctrl+K**. Use **↑ / ↓** to focus
results, **Enter** to open one, and **Escape** to close the dialog and return focus
to the search button, including when the query field contains text.

In automation, Astro may start a background server. Stop the matching project
server with `npm --prefix apps/docs run preview -- stop` or
`npm --prefix apps/docs run dev -- stop`. For a foreground process managed by
another supervisor, append `-- --ignore-lock`; the supervisor then owns shutdown.

## Publish at a domain root

Configure the deployment origin with `DOCS_SITE`. It must be the actual public origin, including the scheme. It is build-time configuration for canonical URLs and sitemap entries, not a KV connection string. Never put a password or token in it.

A portable interactive shell workflow avoids baking an invented production hostname into the repository:

```sh
printf 'Public documentation origin (https:// followed by your host): '
IFS= read -r DOCS_SITE
export DOCS_SITE
export DOCS_BASE=/
npm --prefix apps/docs ci
npm --prefix apps/docs run check
npm --prefix apps/docs run build
```

Publish the **contents of `apps/docs/dist`** at the root of your static host. The default development origin is `http://localhost:4321`; set your real origin before publishing, or canonical URLs and the sitemap will still point to localhost.

For a hosted build service, use these settings:

| Setting | Value |
| --- | --- |
| Checkout/build root | Repository root, not `apps/docs` alone |
| Node | 22.23.4 or newer |
| Install | `npm --prefix apps/docs ci` |
| Build | `npm --prefix apps/docs run build` |
| Publish directory | `apps/docs/dist` |
| Environment | `DOCS_SITE` and, when needed, `DOCS_BASE` |

The generator reads repository metadata, Rust extension registrations and BDD features outside `apps/docs`. Deploying a checkout of only the frontend directory does not provide these inputs.

## Publish under a project subpath

For a host mounting the site under `/ffkv/`, set the base **before building**:

```sh
export DOCS_BASE=/ffkv/
npm --prefix apps/docs run build
npm --prefix apps/docs run preview
```

Visit the preview's **`/ffkv/`** path. Navigation, Markdown links, the command explorer, assets, search and canonical paths must remain inside this prefix. Set `DOCS_SITE` to the host origin, not the host plus the same subpath; `DOCS_BASE` supplies the path once.

Mount the contents of `dist` at that prefix on the production host. A build made for `/ffkv/` is not interchangeable with a domain-root build. Rebuild after changing either environment variable. The integrity audit uses the same variables; preserve them when running `npm --prefix apps/docs run check:site` separately.

## Static hosting behavior

Serve directories through their `index.html`, and serve `404.html` with **HTTP 404** for unknown routes. Do not configure a single-page-app rewrite of all requests to the homepage: these are real static routes, and a missing command should remain a genuine 404.

Keep the entire output, including `_astro`, `pagefind`, sitemap and favicon assets. Pagefind runs in the browser and fetches its local index/chunks; dropping that directory breaks production search. Use HTTPS on the public host, compress text assets, and allow `.wasm` with the `application/wasm` content type when the search build emits WebAssembly.

No production host account, domain, certificate or deployment credential is configured by this repository. Publication remains an operator action; the site build and preview need none of those credentials.

## Maintain the content

Handwritten guides live in `apps/docs/src/content/docs`. Each has a frontmatter `title` and `description`; Starlight renders the page heading. Author internal Markdown links from the documentation root with a trailing slash, such as `/operations/persistence/`. The build's link transform adds `DOCS_BASE` automatically. Astro modules use `docsLink` from `src/lib/links.ts`.

The command reference is generated automatically before `dev`, `check` and `build`:

```sh
npm --prefix apps/docs run generate
```

Inputs include:

- `src/command_catalog.json`: the pinned 486-entry catalog and canonical syntax.
- `src/command_metadata.json`: attributed upstream argument/flag/key metadata.
- Rust registrations and the curated source-backed extension definitions in `apps/docs/scripts`.
- `tests/bdd/features`: real scenario names, argument vectors and source assertions.

Do not edit generated command pages, the generated extension page or `src/data/commands.json`. Update source metadata or the relevant generator helper, regenerate, then review the result. The generator rejects duplicate names/slugs, missing catalog coverage and unknown extension registrations. It only replaces or removes content carrying its ownership marker; it does not delete handwritten pages.

A documentation build **does not execute the KV BDD scenarios**. Extracted examples identify source evidence and may intentionally fail or depend on fixtures. See [behavioral testing](/contributing/testing/) for the real full-catalog runtime gate.

## Continuous integration

`.github/workflows/docs.yml` installs the locked dependencies, runs the content/type checks, builds and audits both root and `/ffkv/` variants, and uploads the static outputs as artifacts. It does not publish to a hosting account or change a running datastore. The same commands above reproduce the work locally.

See [installation](/getting-started/installation/) to run ffkv itself and [the console guide](/getting-started/console/) for the authenticated application UI.
