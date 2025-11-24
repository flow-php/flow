# turnstile_controller.js

Cloudflare Turnstile CAPTCHA integration for bot protection.

## Public Methods

### `getToken(): Promise<string>`
Triggers Turnstile challenge and returns verification token.

**Returns:** Promise resolving to Turnstile token string

**Throws:** Error if Turnstile not initialized or verification fails

**Note:** In test environment (`environment: 'test'`), returns mock token `'test-bypass-token'` without challenge.

### `reset(): void`
Resets Turnstile widget state. Should be called after failed verification before retry.

### `isLoaded(): boolean`
Returns true if widget is initialized.

### `onLoad(): Promise<void>`
Returns promise that resolves when widget is ready.

## Events Dispatched

None

## Value Configuration

- `environment` - Environment mode: `'prod'` or `'test'` (default: `'prod'`)
- `siteKey` - Cloudflare Turnstile site key
- `appearance` - Widget appearance: `'interaction-only'` (default), `'always'`
- `size` - Widget size: `'normal'` (default), `'compact'`

## Behavior

### Production Mode (`environment: 'prod'`)
- Renders visible Turnstile widget
- Requires user interaction to complete challenge
- Returns actual verification token

### Test Mode (`environment: 'test'`)
- Skips widget rendering
- Returns mock token without user interaction
- Used for automated testing

## File: `turnstile_controller.js`
