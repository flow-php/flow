# code_editor_controller.js

CodeMirror-based PHP editor with syntax highlighting and error display.

## Public Methods

### `getCode(): string`
Returns current editor content.

### `setCode(code: string): void`
Replaces editor content with provided code.

### `setValue(code: string): void`
Alias for `setCode()`.

### `highlightError(errorInfo: { line, type, message, column }): void`
Highlights error line in editor and scrolls to it.

**Parameters:**
- `errorInfo.line` - Line number (1-indexed)
- `errorInfo.type` - Error type (e.g., "ParseError", "TypeError")
- `errorInfo.message` - Error message
- `errorInfo.column` - Column number (optional)

### `clearErrors(): void`
Removes all error highlights from editor.

### `isLoaded(): boolean`
Returns true if editor is initialized and ready.

### `onLoad(): Promise<void>`
Returns promise that resolves when editor is ready.

## Events Dispatched

### `code-editor:code-changed`
**Trigger:** User edits code in editor
**Payload:** None
**Bubbles:** Yes

## Features

- PHP syntax highlighting (CodeMirror PHP mode)
- Flow PHP DSL autocompletion:
  - DataFrame methods
  - Scalar function chains
  - DSL helper functions
- Tab key indentation support
- Error line highlighting with hover messages
- Syncs with hidden textarea for form submission

## File: `code_editor_controller.js`
