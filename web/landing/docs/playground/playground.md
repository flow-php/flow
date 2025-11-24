# playground_controller.js

Main orchestrator and event hub for the playground.

## Outlets
- `wasm` - PHP WASM runtime
- `code-editor` - Code editor
- `turnstile` - CAPTCHA widget
- `playground-output` - Output display

## Public Methods

### `onLoad(): Promise<void>`
Waits for all outlets to be loaded. Returns promise that resolves when wasm, code-editor, turnstile, and playground-output are ready.

### `isLoaded(): boolean`
Returns true if all outlets are loaded and ready.

### `showLoading(message: string, percent: number): void`
Displays loading progress with message and percentage.

### `hideLoading(): void`
Hides loading indicators and shows main UI.

### `previewFile(event: Event): Promise<void>`
Preview file from workspace. Reads file via WASM, applies syntax highlighting with Prism, and displays in preview panel.

**Parameters:**
- `event.currentTarget.dataset.filePath` - Path relative to `/workspace/`

### `closeFilePreview(event?: Event): void`
Closes the file preview panel.

### `downloadPreviewFile(event?: Event): void`
Downloads currently previewed file to user's computer.

## Events Listened

### `playground-storage:loaded-from-storage`
**Handler:** `onStorageLoaded()`
**Action:** Shows "Loaded from local storage" indicator

### `playground-share:loaded-from-url`
**Handler:** `onUrlLoaded()`
**Action:** Shows "Loaded from snippet" indicator

### `playground-share:datasets-loaded`
**Handler:** `onDatasetsLoaded()`
**Action:** Logs datasets loaded from R2

### `wasm:initialized`
**Handler:** `onWasmInitialized()`
**Action:** Hides loading, shows "Click Run" info message

### `wasm:loading-progress`
**Handler:** `onWasmLoadingProgress(event)`
**Payload:** `{ message: string, percent: number }`
**Action:** Updates loading bar and message

### `wasm:output`
**Handler:** `onWasmOutput(event)`
**Payload:** `{ output: string }`
**Action:** Displays output in output panel

### `wasm:error`
**Handler:** `onWasmError(event)`
**Payload:** `{ error: string, errorInfo: { line, type, message } }`
**Action:** Displays error, highlights line in editor, hides loading

### `playground-share:notification`
**Handler:** `onNotification(event)`
**Payload:** `{ message: string, type: string, link?: string }`
**Action:** Displays notification via output panel

## Events Dispatched

None (acts as event hub, routes events through outlets)

## File: `playground_controller.js`
