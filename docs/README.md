# go-oversync Documentation

This directory contains the Jekyll-based documentation for go-oversync, automatically published to GitHub Pages.

## Local Development

To run the documentation locally:

```bash
cd docs
bundle install
bundle exec jekyll serve
```

The site will be available at `http://localhost:4000/go-oversync/`

## Automatic Publishing

The documentation is automatically published to GitHub Pages via GitHub Actions when:

- Changes are pushed to the `main` branch in the `docs/` directory
- Pull requests are opened that modify the `docs/` directory (build-only, no deployment)

The workflow:
1. **Builds** the Jekyll site using the same environment as GitHub Pages
2. **Tests** that the build succeeds on pull requests
3. **Deploys** to GitHub Pages on pushes to main

## Site Structure

- `index.markdown` - Home page with overview and features
- `getting-started.md` - Complete tutorial for building a sync-enabled server
- `documentation.md` - Links to detailed API and implementation docs
- `documentation/` - Detailed documentation pages
  - `api.md` - HTTP API reference
  - `server.md` - Server integration guide
  - `client.md` - Client library documentation

## Configuration

The site is configured in `_config.yml` with:
- **Base URL**: `/go-oversync` (for GitHub Pages)
- **Theme**: Minima (GitHub Pages compatible)
- **Plugins**: Jekyll Feed for RSS, Remote Theme support
- **Navigation**: Automatic header navigation from `header_pages`

## GitHub Pages Setup

To enable GitHub Pages for this repository:

1. Go to repository **Settings** → **Pages**
2. Set **Source** to "GitHub Actions"
3. The workflow will automatically deploy on the next push to main

The documentation will be available at: `https://mobiletoly.github.io/go-oversync/`
