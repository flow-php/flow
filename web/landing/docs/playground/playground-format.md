# playground_format_controller.js

Format PHP code using CS-Fixer via WASM.

## Outlets
- `code-editor` - Code editor
- `wasm` - PHP WASM runtime
- `playground-output` - Output display

## Public Methods

### `format(): Promise<void>`
Formats code from editor using PHP CS Fixer.

**Process:**
1. Waits for editor and WASM to load
2. Gets code from editor
3. Writes code to `/workspace/code.php` in WASM filesystem
4. Shows "Formatting..." message
5. Disables format button
6. Calls WASM format method (formats `/workspace/code.php` in place)
7. Updates editor with formatted code on success
8. Shows applied fixers if any
9. Re-enables format button
10. Dispatches formatted event

## Events Dispatched

### `playground-format:formatted`
**Trigger:** Code formatting completed (success or failure)
**Payload:** `{ success: boolean }`
**Bubbles:** Yes

## File: `playground_format_controller.js`
