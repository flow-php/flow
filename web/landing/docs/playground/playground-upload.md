# playground_upload_controller.js

Upload datasets to WASM filesystem for use in code execution.

## Outlets
- `wasm` - PHP WASM runtime
- `playground-output` - Output display

## Public Methods

### `triggerUpload(event: Event): void`
Opens browser file picker dialog.

### `uploadFile(file: File): Promise<void>`
Validates and uploads single file to `/workspace/uploads/`.

**Validation:**
- Filename must be safe (alphanumeric, dash, underscore, dot, parentheses, spaces only)
- No path traversal attempts (`..`, `/`, `\`)
- Max 255 characters filename
- Must have extension
- Max file size: 2MB
- Allowed extensions: csv, json, xml, php, phar

### `handleFileUpload(event: Event): Promise<void>`
Handles file input change event. Uploads all selected files sequentially.

### `listFiles(): Promise<Array>`
Lists all files in `/workspace/uploads/` directory.

**Returns:** Array of file objects from WASM

## Events Dispatched

### `playground-upload:file-uploaded`
**Trigger:** File successfully written to WASM filesystem
**Payload:** `{ filename: string, size: number }`
**Bubbles:** Yes

## Value Configuration

- `maxFileSize` - Maximum file size in bytes (default: 2097152 = 2MB)
- `maxFileCount` - Maximum number of files (default: 3)
- `allowedExtensions` - Array of allowed file extensions (default: `['csv', 'json', 'xml', 'php', 'phar']`)

## File: `playground_upload_controller.js`
