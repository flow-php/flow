# Cloudflare Workers - Local Development

## Prerequisites

- Nix shell environment (includes Wrangler CLI)

## Setup

1. Enter the nix-shell (if not already in it):
```bash
nix-shell
```

2. Create local environment file:
```bash
cp .dev.vars.example .dev.vars
```

3. Edit `.dev.vars` and add your Turnstile secret key:
```
TURNSTILE_SECRET_KEY=your-actual-secret-key
```

You can get the Turnstile secret key after running `terraform apply`:
```bash
cd ..
terraform output -raw turnstile_secret_key
```

## Local Development

Start the local development server:
```bash
cd terraform/cloudflare/workers
wrangler dev
```

This will start Wrangler's local development server at `http://localhost:8787`

## Testing the Worker Locally

### Using curl:

```bash
# Test with a valid token (you'll need to get a real token from the Turnstile widget)
curl -X POST http://localhost:8787/api/verify-turnstile \
  -H "Content-Type: application/json" \
  -d '{"token": "your-turnstile-token-here"}'
```

### Using a test HTML page:

Create a `test.html` file:

```html
<!DOCTYPE html>
<html>
<head>
    <title>Turnstile Test</title>
    <script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
</head>
<body>
    <h1>Turnstile Test</h1>
    <div class="cf-turnstile" data-sitekey="YOUR_SITE_KEY" data-callback="onTurnstileSuccess"></div>

    <script>
        function onTurnstileSuccess(token) {
            console.log('Turnstile token:', token);

            // Test with local worker
            fetch('http://localhost:8787/api/verify-turnstile', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ token })
            })
            .then(r => r.json())
            .then(data => {
                console.log('Verification result:', data);
                alert('Success: ' + data.success);
            })
            .catch(err => {
                console.error('Error:', err);
            });
        }
    </script>
</body>
</html>
```

Replace `YOUR_SITE_KEY` with your actual Turnstile site key from:
```bash
cd ..
terraform output turnstile_site_key
```

## Deployment

The worker is deployed via Terraform, not directly via Wrangler. To deploy changes:

1. Edit `turnstile-verify.js`
2. Run `terraform apply` from the `terraform/cloudflare` directory

## Debugging

View logs in the Wrangler dev session by checking the terminal output. All console.log statements will appear there.

## Notes

- The `.dev.vars` file contains secrets and is gitignored
- Local development uses Wrangler's local runtime, which closely mimics the production Cloudflare Workers environment
- CORS is configured to allow requests from `https://flow-php.com` - you may need to adjust for local testing
