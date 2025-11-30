# playground_workspace_controller.js

Display and navigate the WASM virtual filesystem.

## Outlets
- `wasm` - PHP WASM runtime
- `code-editor` - Code editor
- `playground` - Main playground controller

## Targets
- `tree` - Container for the file tree (desktop view)
- `dropdown` - Select element for file selection (mobile view)
- `emptyState` - Empty state message when no files

## Public Methods

### `refreshTree(): Promise<void>`
Rebuilds file tree from WASM filesystem.

**Process:**
1. Waits for playground outlets to load
2. Lists all files in `/workspace/` recursively via WASM
3. Filters files under `/workspace/`
4. Builds hierarchical tree structure
5. Renders tree with folders and files
6. Dispatches refreshed event

### `isLoaded(): boolean`
Returns true if tree has been rendered at least once.

### `onLoad(): Promise<void>`
Returns promise that resolves when tree is rendered for first time.

## Events Listened

### `wasm:file-created`
**Handler:** `refreshTree()`
**Action:** Refreshes file tree

### `wasm:file-deleted`
**Handler:** `refreshTree()`
**Action:** Refreshes file tree

### `wasm:file-updated`
**Handler:** `refreshTree()`
**Action:** Refreshes file tree

### `playground-upload:file-uploaded`
**Handler:** `refreshTree()`
**Action:** Refreshes file tree

### `playground-upload:files-cleared`
**Handler:** `refreshTree()`
**Action:** Refreshes file tree

## Events Dispatched

### `playground-workspace:tree-rendered`
**Trigger:** File tree rendered/updated
**Payload:** `{ fileCount: number, folderCount: number }`
**Bubbles:** Yes

### `playground-workspace:refreshed`
**Trigger:** Tree refresh completed
**Payload:** `{ fileCount: number }`
**Bubbles:** Yes

## Value Configuration

- `rootPath` - Root directory to display (default: `/workspace`)
- `folderIcon` - URL to folder icon image
- `fileIcon` - URL to file icon image

## Behavior

- Files sorted alphabetically
- Folders displayed before files
- Clicking file in tree opens it via `playground-tabs#openFile()`
- Selecting file in dropdown (mobile) opens it via `playground-tabs#openFileFromDropdown()`
- Tree and dropdown auto-refresh on filesystem changes
- Shows empty state when no files present

## File: `playground_workspace_controller.js`
