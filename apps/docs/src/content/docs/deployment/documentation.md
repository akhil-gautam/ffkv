---
title: Documentation site
description: Run, maintain and deploy the ffkv Astro handbook as static files, independently of the KV server and management console.
sidebar:
  order: 3
---

The handbook is a separate **Astro + Starlight** application in `apps/docs`. It builds to static HTML, CSS, JavaScript, a local Pagefind search index and the [browser playground](/playground/)'s WebAssembly engine. It does not require a running ffkv instance, a Redis server, the React console, a hosted search service or a database.

## Run locally

Use Node **22.23.4 or newer** and npm, plus Rust with the `wasm32-wasip1` target (`rustup target add wasm32-wasip1`) for the playground engine. From the repository root:

```sh
npm --prefix apps/docs ci
npm --prefix apps/docs run dev
```

Open `http://localhost:4321`. The development server binds loopback and refuses an occupied port instead of silently using a different one. Stop only this process with Ctrl-C. It does not manage any KV processes or data directories.

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

`check`, `dev` and `build` first compile the playground engine (`cargo build --release --target wasm32-wasip1 -p ffkv-wasm`, copied to the gitignored `apps/docs/public/playground/ffkv.wasm`), because the reference generator uses it. `build` then runs `scripts/playground-smoke.mjs`, which loads the engine in Node, checks real command replies and replays every generated "Try in playground" sequence on an empty dataset. Rerun `npm --prefix apps/docs run build:wasm` after changing Rust sources while the dev server runs. `check` runs Astro's content and TypeScript diagnostics. `build` creates `apps/docs/dist`, builds Pagefind, and audits **every generated HTML page** for page metadata, a single page heading, duplicate IDs, local links/assets and fragment destinations. It also checks every canonical command route, the production search index and the playground engine. A broken reference fails the build.

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

## Cloudflare production deployment

The public handbook is hosted at **[ffkv.pages.dev](https://ffkv.pages.dev/)** using [Cloudflare Pages](https://developers.cloudflare.com/pages/configuration/serving-pages/).

`apps/docs/wrangler.jsonc` pins the `ffkv` Pages project and its static output directory. The `build:pages` and `deploy:pages` scripts in `apps/docs/package.json` pin the production origin and Cloudflare account. This deployment serves only generated documentation; it does not deploy the KV server, management console or repository source.

From the repository root, install the locked documentation dependencies and deploy:

```sh
npx --yes --package node@22.23.4 -- npm --prefix apps/docs ci
npx --yes --package node@22.23.4 -- npm --prefix apps/docs run deploy:pages
```

On a machine not yet authenticated to the deployment account, first run:

```sh
npx --yes --package node@22.23.4 --package wrangler@4.156.1 -- wrangler login
```

`deploy:pages` runs `build:pages`, which sets `DOCS_SITE=https://ffkv.pages.dev` and `DOCS_BASE=/`, regenerates the reference, builds the search index and runs the full site integrity audit. Only a successful build is uploaded to the Pages project's `main` production branch. Pages serves directory indexes at trailing-slash routes; the generated top-level `404.html` supplies real HTTP 404 responses for missing paths instead of an SPA fallback.

Deployment is manual: pushing to GitHub runs documentation CI but does not publish to Cloudflare. To deploy a fork or change the public hostname, update the Pages project in `wrangler.jsonc` and the account and build origin in the package scripts together. Authentication stays in Wrangler's local credentials or the deploying environment, never in the repository.

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
| Rust | Stable, with the `wasm32-wasip1` target |
| Install | `npm --prefix apps/docs ci` |
| Build | `npm --prefix apps/docs run build` |
| Publish directory | `apps/docs/dist` |
| Environment | `DOCS_SITE` and, when needed, `DOCS_BASE` |

The generator reads repository metadata, Rust extension registrations and BDD features outside `apps/docs`, and the playground engine is compiled from the Rust sources. Deploying a checkout of only the frontend directory does not provide these inputs.

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

Keep the entire output, including `_astro`, `pagefind`, `playground`, sitemap and favicon assets. Pagefind runs in the browser and fetches its local index/chunks; dropping that directory breaks production search. Use HTTPS on the public host, compress text assets, and serve `.wasm` with the `application/wasm` content type (the playground engine always, the search build when it emits WebAssembly) so browsers can compile it while downloading.

The Cloudflare Pages target is configured in `apps/docs/wrangler.jsonc` and the package deployment scripts; no deployment credential is stored in the repository. Other hosting providers require their own publication setup. Local builds and previews need no hosting credentials.

## Maintain the content

Handwritten guides live in `apps/docs/src/content/docs`. Each has a frontmatter `title` and `description`; Starlight renders the page heading. Author internal Markdown links from the documentation root with a trailing slash, such as `/operations/persistence/`. The build's link transform adds `DOCS_BASE` automatically. Astro modules use `docsLink` from `src/lib/links.ts`.

Keep local development URL examples in inline code rather than clickable links. A localhost example can share the development build's origin while falling outside a configured project subpath; it is an instruction for the reader, not site navigation.

The command reference is generated automatically before `dev`, `check` and `build`:

```sh
npm --prefix apps/docs run generate
```

Inputs include:

- `src/command_catalog.json`: the pinned 486-entry catalog and canonical syntax.
- `src/command_metadata.json`: attributed upstream argument/flag/key metadata.
- Rust registrations and the curated source-backed extension definitions in `apps/docs/scripts`.
- `tests/bdd/features`: real scenario names, argument vectors and source assertions.
- `src/wasm/session.rs` and the built playground engine: which commands the playground refuses, and replay checks for each command page's "Try in playground" sequence (the scenario's setup commands, then the example). A sequence is kept only if no command fails or blocks on an empty dataset, and setup steps are dropped when the example's reply is byte-identical without them; otherwise the link pre-fills just the command name.

Do not edit generated command pages, the generated extension page or `src/data/commands.json`. Update source metadata or the relevant generator helper, regenerate, then review the result. The generator rejects duplicate names/slugs, missing catalog coverage and unknown extension registrations. It only replaces or removes content carrying its ownership marker; it does not delete handwritten pages.

A documentation build **does not execute the KV BDD scenarios** against a server. Extracted examples identify source evidence and may intentionally fail or depend on fixtures; only the playground sequences are replayed, in the browser engine. See [behavioral testing](/contributing/testing/) for the real full-catalog runtime gate.

## Continuous integration

`.github/workflows/docs.yml` installs the locked dependencies, runs the content/type checks, builds and audits both root and `/ffkv/` variants, and uploads the static outputs as artifacts. It does not publish to a hosting account or change a running datastore. The same commands above reproduce the work locally.

See [installation](/getting-started/installation/) to run ffkv itself and [the console guide](/getting-started/console/) for the authenticated application UI.
