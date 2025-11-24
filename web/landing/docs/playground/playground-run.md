# playground_run_controller.js

Execute PHP code in the WASM runtime.

## Outlets
- `code-editor` - Code editor
- `wasm` - PHP WASM runtime
- `playground-output` - Output display

## Public Methods

### `run(): Promise<void>`
Executes code from editor in WASM runtime.

**Process:**
1. Waits for editor and WASM to load
2. Gets code from editor
3. Writes code to `/workspace/code.php` in WASM filesystem
4. Clears previous errors and output
5. Disables run button during execution
6. Executes code via WASM
7. Displays result (success or error)
8. Highlights error line if present
9. Re-enables run button
10. Dispatches execution event

## Events Dispatched

### `playground-run:executed`
**Trigger:** Code execution completed (success or failure)
**Payload:** `{ success: boolean, executionTime: number }`
**Bubbles:** Yes

## File: `playground_run_controller.js`
