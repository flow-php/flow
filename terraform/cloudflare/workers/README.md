# Cloudflare Workers - Snippet Upload

## Local Development

### Setup

1. **Create `.dev.vars` file** (gitignored):
   ```bash
   cd terraform/cloudflare/workers
   cp .dev.vars.example .dev.vars
   ```

2. **Edit `.dev.vars`** (sets Turnstile mode):
   ```bash
   TURNSTILE_SECRET_KEY=dummy
   TURNSTILE_MODE=bypass
   ```

   **Turnstile Modes**:
   - `bypass` - Skip verification entirely (fastest, no widget interaction)
   - `mock` - Accept any token without API call (test full widget flow)
   - `mock-fail` - Reject any token (test error handling)
   - `verify` - Full production verification (requires real secret key)

### Start Local Worker

```bash
cd terraform/cloudflare/workers
wrangler dev
```

This starts the worker at `http://localhost:8787` with local R2 emulation.

### Configure Frontend

To test with the local playground, configure `web/landing/.env`:

```bash
PLAYGROUND_API_URL=http://localhost:8787/api/playground/snippets
PLAYGROUND_SNIPPETS_URL=http://localhost:8787
TURNSTILE_SITE_KEY=0x4AAAAAACAjJaWImyaJwWCy
TURNSTILE_APPEARANCE=always
```

**Turnstile Appearance Modes**:
- `interaction-only` - Only shows challenge when suspicious (default, production behavior)
- `always` - Always shows visible challenge (best for local testing with mock mode)
- `execute` - Hidden unless explicitly triggered

Then open `https://flow-php.wip/playground` and use the Share button to test uploads.

## Deployment

**All deployments are managed by Terraform**, not Wrangler:

```bash
cd terraform/cloudflare
terraform apply
```

Changes to `workers/*.js` files trigger automatic worker updates in Terraform.
