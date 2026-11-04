# playground_format_controller.js

Format PHP code with the Mago formatter, the official Mago WASM build committed in `assets/wasm/mago/` (`mago_wasm_bg.js` and `mago_wasm_bg.wasm` from the `mago-<version>-wasm.tar.gz` release asset).

## Outlets
- `code-editor` - Code editor
- `wasm` - PHP WASM runtime
- `playground-output` - Output display

## Public Methods

### `format(): Promise<void>`
Formats code from editor using Mago, the same formatter `just fix` runs on the repository.

**Process:**
1. Waits for editor and WASM to load
2. Shows "Formatting..." message
3. Loads the Mago WASM module on first use (`assets/services/mago.js`)
4. Formats the editor code for PHP 8.5 and puts the result back into the editor
5. Writes the formatted code to `/workspace/code.php` in the WASM filesystem
6. Shows the parse error when the code cannot be formatted
7. Dispatches formatted event

## Values
- `magoWasm` - URL to `mago_wasm_bg.wasm`
- `phpVersion` - PHP version the code targets, `8.5` by default

## Events Dispatched

### `playground-format:formatted`
**Trigger:** Code formatting completed (success or failure)
**Payload:** `{ success: boolean }`
**Bubbles:** Yes

## File: `playground_format_controller.js`
