# wasm_controller.js

PHP WebAssembly runtime for executing PHP code in the browser.

## Public Methods

### `run(code: string): Promise<Result>`
Execute PHP code and return result.

**Returns:**
```javascript
{
  success: boolean,
  output?: string,           // stdout/stderr combined
  error?: {
    type: string,           // Error type (ParseError, TypeError, etc.)
    message: string,
    line?: number,
    column?: number,
    severity: 'error'
  },
  exitCode: number,
  executionTime: number,
  memoryUsed: number
}
```

### `format(code: string): Promise<Result>`
Format PHP code using CS-Fixer.

**Returns:**
```javascript
{
  success: boolean,
  code?: string,            // Formatted code
  error?: string,
  fixers?: string[]         // Applied fixer names
}
```

### `readFile(path: string): Promise<Result>`
Read file from WASM filesystem.

**Returns:**
```javascript
{
  success: boolean,
  content?: string | Uint8Array,
  error?: string
}
```

### `writeFile(path: string, content: string | Uint8Array): Promise<Result>`
Write file to WASM filesystem. Creates parent directories if needed.

**Returns:**
```javascript
{
  success: boolean,
  bytesWritten: number,
  error?: string
}
```

### `listFiles(path: string = '/workspace', recursive: boolean = false): Promise<Result>`
List files in directory.

**Returns:**
```javascript
{
  success: boolean,
  files: Array<{
    name: string,
    path: string,
    type: 'file' | 'directory'
  }>
}
```

### `removeFile(path: string): Promise<Result>`
Delete file from filesystem.

### `isLoaded(): boolean`
Returns true if PHP module is initialized.

### `onLoad(): Promise<void>`
Returns promise that resolves when PHP module is ready.

### `areResourcesLoaded(): boolean`
Returns true if playground resources (PHARs, datasets) are loaded.

### `getModule(): EmscriptenModule`
Returns raw Emscripten module for advanced operations.

## Events Dispatched

### `wasm:ready`
**Trigger:** PHP module loaded (before resources)
**Payload:** None

### `wasm:initialized`
**Trigger:** PHP module and resources fully loaded
**Payload:** None

### `wasm:resources-loaded`
**Trigger:** Resources loaded into filesystem
**Payload:** None

### `wasm:loading-progress`
**Trigger:** Resource loading progress
**Payload:** `{ message: string, percent: number }`

### `wasm:progress`
**Trigger:** PHP module initialization progress
**Payload:** `{ message: string, percent: number }`

### `wasm:output`
**Trigger:** Code executed successfully
**Payload:** `{ output: string }`

### `wasm:error`
**Trigger:** Code execution error
**Payload:** `{ error: string, errorInfo?: { line, type, message, column } }`

### `wasm:file-created`
**Trigger:** New file written to filesystem
**Payload:** `{ path: string, size: number }`
**Bubbles:** Yes

### `wasm:file-updated`
**Trigger:** Existing file updated
**Payload:** `{ path: string, size: number }`
**Bubbles:** Yes

### `wasm:file-deleted`
**Trigger:** File removed from filesystem
**Payload:** `{ path: string }`
**Bubbles:** Yes

## Filesystem Structure

```
/workspace/               # Main working directory (chdir on execution)
  code.php                # Current playground code (loaded as resource, synced on run/format/share)
  /uploads/               # User-uploaded datasets
  /bin/
    cs-fixer.php          # PHP CS Fixer PHAR
  /vendor/                # Flow PHP libraries
  /data/                  # Sample datasets (CSV, JSON, XML)
```

## Value Configuration

- `phpJs` - URL to php.js loader script
- `phpWasm` - URL to php.wasm binary
- `resources` - Object mapping virtual paths to asset URLs

## File: `wasm_controller.js`
