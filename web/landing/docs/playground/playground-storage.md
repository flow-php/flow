# playground_storage_controller.js

Persist code to browser localStorage with auto-save.

## Outlets
- `code-editor` - Code editor

## Public Methods

### `loadCode(): Promise<void>`
Loads saved code from localStorage and sets it in editor.

**Note:** Automatically called when code-editor outlet connects, unless URL contains `?snippet=` parameter.

### `clearCode(): void`
Removes saved code from localStorage.

## Events Listened

### `code-editor:code-changed`
**Handler:** `#handleCodeChanged()`
**Action:** Triggers debounced auto-save (1000ms default)

## Events Dispatched

### `playground-storage:loaded`
**Trigger:** Code loaded from localStorage
**Payload:** `{ codeLength: number }`
**Bubbles:** Yes

### `playground-storage:loaded-from-storage`
**Trigger:** Code loaded from localStorage (no payload, for UI indicators)
**Payload:** None
**Bubbles:** Yes

### `playground-storage:saved`
**Trigger:** Code auto-saved to localStorage
**Payload:** `{ codeLength: number }`
**Bubbles:** Yes

### `playground-storage:cleared`
**Trigger:** localStorage cleared
**Payload:** `{ timestamp: number }`
**Bubbles:** Yes

## Value Configuration

- `debounceMs` - Auto-save debounce delay in milliseconds (default: 1000)
- `storageKey` - localStorage key name (default: `'flow-playground-code'`)

## Behavior

- Auto-saves code changes after 1 second of inactivity
- Skips loading if `?snippet=` parameter present in URL
- Stores code in browser localStorage (survives page reloads)
- Clears storage on playground reset

## File: `playground_storage_controller.js`
