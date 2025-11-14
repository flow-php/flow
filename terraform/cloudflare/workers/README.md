# Cloudflare Workers - Snippet Upload

## Local Development

### Setup

1. **Create `.dev.vars` file** (gitignored):
   ```bash
   cd terraform/cloudflare/workers
   cp .dev.vars.example .dev.vars
   ```

2. **Edit `.dev.vars`**:
   ```bash
   TURNSTILE_SECRET_KEY=dummy
   ENVIRONMENT=development
   ```

### Start Local Worker

```bash
cd terraform/cloudflare/workers
wrangler dev
```

This starts the worker at `http://localhost:8787` with local R2 emulation.

**Note**: Development mode (`ENVIRONMENT=development`) bypasses Turnstile verification for easier testing.

### Configure Frontend

To test with the local playground, configure `web/landing/.env.local`:

```bash
PLAYGROUND_API_URL=http://localhost:8787/api/playground/snippets
PLAYGROUND_SNIPPETS_URL=http://localhost:8787
TURNSTILE_SITE_KEY=0x4AAAAAACAjJaWImyaJwWCy
```

Then open `https://flow-php.wip/playground` and use the Share button to test uploads.

## Deployment

**All deployments are managed by Terraform**, not Wrangler:

```bash
cd terraform/cloudflare
terraform apply
```

Changes to `workers/*.js` files trigger automatic worker updates in Terraform.
