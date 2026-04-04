# Documentation

This directory contains the Jekyll source for the go-oversync documentation site.

The docs should describe the current repository state only. If code and docs disagree, update the
docs to match the verified runtime, tests, examples, or shipped API contract.

## Local Development

Build the site once:

```bash
cd docs
./build-site
```

Serve it locally:

```bash
cd docs
BUNDLE_PATH=vendor/bundle bundle install
RUBYOPT="-r./_plugins/ruby_compat.rb" BUNDLE_PATH=vendor/bundle bundle exec jekyll serve
```

Default local URL:

```text
http://localhost:4000/go-oversync/
```

## Site Structure

- `index.markdown`: landing page
- `getting-started.md`: current setup guide
- `architecture.md`: bundle-based architecture overview
- `documentation/core-concepts.md`: vocabulary and end-to-end model
- `documentation/server.md`: PostgreSQL/server runtime overview
- `documentation/server-originated-writes.md`: `ScopeManager` / `WithinSyncBundle` guidance
- `documentation/client.md`: SQLite client runtime contract
- `documentation/api.md`: HTTP API reference
- `documentation/advanced-concepts.md`: deeper contract notes
- `documentation/performance.md`: current operational hotspots
- `_data/navigation.yml`: top navigation

## Maintenance Rule

Avoid documenting:

- planned features as if they exist
- historical workflows unless they still matter to current users
- counts, guarantees, or CI claims unless they are verified in the current repo state
