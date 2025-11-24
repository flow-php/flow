# playground_share_controller.js

Share code snippets via Cloudflare R2 storage with public URLs.

## Outlets
- `code-editor` - Code editor
- `wasm` - PHP WASM runtime
- `turnstile` - CAPTCHA widget
- `playground-upload` - File upload controller

## Public Methods

### `share(): Promise<void>`
Creates shareable snippet by uploading code and datasets to API.

**Process:**
1. Waits for all outlets to load
2. Gets code from editor
3. Writes code to `/workspace/code.php` in WASM filesystem
4. Obtains Turnstile verification token
5. Lists uploaded datasets from playground-upload
6. Reads dataset files from WASM
7. Creates FormData with code and up to 3 datasets
8. POSTs to API with Turnstile token in header
9. Receives snippet ID from API response
10. Updates browser URL to `?snippet={id}`
11. Copies share URL to clipboard
12. Shows success notification

**API Request:**
```
POST /api/playground/snippets
Headers:
  CF-Turnstile-Response: {token}
Body (multipart/form-data):
  code: code.php (blob)
  dataset_1: {filename} (optional)
  dataset_2: {filename} (optional)
  dataset_3: {filename} (optional)
```

**API Response:**
```javascript
{
  success: true,
  snippet_id: string,
  url: string,
  expires_at: string
}
```

### `loadCodeFromUrl(): void`
Loads snippet from R2 when `?snippet=` parameter present in URL.

**Process:**
1. Gets snippet ID from URL query parameter
2. Fetches `snippet.json` metadata from R2
3. Checks expiration date
4. Fetches `code.php` from R2
5. Sets code in editor
6. Writes code to `/workspace/code.php` in WASM filesystem
7. If datasets present:
   - Waits for WASM resources to load
   - Fetches each dataset file from R2
   - Writes to `/workspace/uploads/` via WASM
8. Updates UI with "Loaded from snippet" indicator

**R2 URLs:**
- Metadata: `{snippetsUrl}/snippets/{id}/snippet.json`
- Code: `{snippetsUrl}/snippets/{id}/code.php`
- Datasets: `{snippetsUrl}/snippets/{id}/datasets/{filename}`

### `onWasmResourcesLoaded(): void`
Internal handler called when WASM resources finish loading. Triggers pending snippet load if deferred.

## Events Listened

### `code-editor:code-changed`
**Handler:** `#handleCodeChanged()`
**Action:** Clears `?snippet=` from URL when user edits loaded snippet

### `wasm:resources-loaded`
**Handler:** `onWasmResourcesLoaded()`
**Action:** Loads pending snippet if deferred

## Events Dispatched

### `playground-share:notification`
**Trigger:** User-facing notification (success, error, warning)
**Payload:** `{ message: string, type: string, link?: string }`
**Bubbles:** Yes

### `playground-share:datasets-loaded`
**Trigger:** All snippet datasets loaded from R2
**Payload:** None
**Bubbles:** Yes

### `playground-share:loaded-from-url`
**Trigger:** Snippet loaded from URL successfully
**Payload:** None
**Bubbles:** Yes

## Value Configuration

- `apiUrl` - Snippet upload API endpoint
- `snippetsUrl` - Base URL for R2 public bucket

## Snippet Structure

R2 stores snippets in this structure:
```
/snippets/{id}/
  snippet.json          # Metadata
  code.php              # PHP code
  datasets/             # Optional datasets
    data.csv
    users.json
```

**snippet.json format:**
```javascript
{
  snippet_id: string,
  created_at: string,     // ISO 8601
  expires_at: string,     // ISO 8601 (90 days)
  files: [
    {
      name: string,
      size: number,
      type: "code" | "dataset"
    }
  ],
  total_size: number,
  ip_hash: string
}
```

## Behavior

- Snippets expire after 90 days
- URL updates to `?snippet={id}` after successful share
- URL clears when user edits loaded snippet
- Share link copied to clipboard automatically
- Datasets limited to 3 files per snippet
- Requires Turnstile token for upload (bot protection)
- Defers snippet loading if WASM resources not ready

## File: `playground_share_controller.js`
