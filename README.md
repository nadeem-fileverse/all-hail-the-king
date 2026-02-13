# Fileverse Cloudflare Worker

One-click deploy the [Fileverse API](https://github.com/fileverse/fileverse-api) to Cloudflare Workers with D1 (SQLite) storage and automatic blockchain sync via cron triggers.

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/fileverse/fileverse-cloudflare-worker)

## What is this?

This template deploys a Cloudflare Worker that:

- Serves the full Fileverse document management API (create, read, update, delete documents)
- Stores documents in Cloudflare D1 (SQLite at the edge)
- Automatically syncs documents to the Gnosis blockchain every minute via cron triggers
- Exposes search, folder management, and event retry endpoints

## Prerequisites

- A Cloudflare account
- A Fileverse API key (get one at [fileverse.io](https://fileverse.io))

## Quick Start

### Option 1: Deploy Button

Click the deploy button above. After deployment:

```bash
# Set your API key as a secret
wrangler secret put API_KEY
```

### Option 2: Manual Setup

```bash
# Clone the repo
git clone https://github.com/fileverse/fileverse-cloudflare-worker.git
cd fileverse-cloudflare-worker

# Install dependencies
npm install

# Create the D1 database
wrangler d1 create fileverse-db
# Copy the database_id from the output into wrangler.toml

# Set up environment
cp .dev.vars.example .dev.vars
# Edit .dev.vars and set your API_KEY

# Apply database migrations
npx wrangler d1 migrations apply DB --local

# Run locally
npm run dev
```

### Verify

```bash
# Health check
curl http://localhost:8787/ping

# Create a document
curl -X POST "http://localhost:8787/api/ddocs?apiKey=YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"title": "Test", "content": "Hello from Cloudflare!"}'

# List documents
curl "http://localhost:8787/api/ddocs?apiKey=YOUR_KEY"

# Trigger cron manually (local dev only)
curl "http://localhost:8787/__scheduled?cron=*+*+*+*+*"
```

## API Endpoints

All `/api/*` endpoints require `?apiKey=YOUR_KEY` query parameter.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/ping` | Health check |
| `POST` | `/api/ddocs` | Create a document |
| `GET` | `/api/ddocs` | List documents (`?limit=&skip=`) |
| `GET` | `/api/ddocs/:ddocId` | Get a document |
| `PUT` | `/api/ddocs/:ddocId` | Update a document |
| `DELETE` | `/api/ddocs/:ddocId` | Delete a document |
| `GET` | `/api/folders` | List folders |
| `POST` | `/api/folders` | Create a folder |
| `GET` | `/api/folders/:folderRef/:folderId` | Get a folder |
| `GET` | `/api/search?q=query` | Search documents |
| `GET` | `/api/events/failed` | List failed sync events |
| `POST` | `/api/events/retry-failed` | Retry all failed events |
| `POST` | `/api/events/:id/retry` | Retry a single event |

## Configuration

### Environment Variables

Set via `wrangler secret put` for production, or in `.dev.vars` for local development.

| Variable | Required | Description |
|----------|----------|-------------|
| `API_KEY` | Yes | Your Fileverse API key |
| `RPC_URL` | No | Custom Gnosis RPC URL |

### Cron Schedule

The worker runs a cron trigger every minute (`*/1 * * * *`) to process pending blockchain sync events. Each invocation processes up to 3 events. Adjust the schedule in `wrangler.toml` if needed.

## Deploying

```bash
npm run deploy
```

This runs D1 migrations and deploys the worker in one step.

## Architecture

```
Request → Cloudflare Worker (Hono)
           ├── Auth middleware (API key check)
           ├── Route handler (calls @fileverse/api domain functions)
           └── D1 Adapter (SQLite-compatible DatabaseAdapter)

Cron (every minute) → Process pending events
                       ├── Fetch next eligible event
                       ├── Call processEvent() (blockchain tx)
                       └── Mark processed/failed
```

The worker imports `@fileverse/api/cloudflare` which provides domain logic, models, and the event processor without pulling in Node-only dependencies (Express, better-sqlite3, etc.).

## MCP Integration

MCP (Model Context Protocol) support is available via the Fileverse CLI tool:

```bash
npx @fileverse/api mcp
```

Direct MCP-over-HTTP in Workers is planned for a future release.

## Troubleshooting

**"Invalid or missing API key"** — Make sure you've set the `API_KEY` secret via `wrangler secret put API_KEY`.

**Documents stuck in "pending" sync status** — Check that the cron trigger is running. View failed events at `GET /api/events/failed?apiKey=YOUR_KEY` and retry with `POST /api/events/retry-failed?apiKey=YOUR_KEY`.

**D1 migration errors** — Run `wrangler d1 migrations apply DB --remote` manually.
