# playground_tabs_controller.js

Manages tabbed interface for switching between code editor and file preview.

## Outlets
- `wasm` - PHP WASM runtime for reading files
- `code-editor` - Code editor instance

## Targets
- `tabBar` - Container for tab buttons
- `codeTab` - Code editor tab button
- `previewTab` - Preview tab button
- `previewTabName` - Span showing preview file name
- `codePanel` - Code editor container
- `previewPanel` - Preview content container
- `downloadBtn` - Download button for preview file

## Values
- `activeTab` (String, default: 'code') - Currently active tab ('code' or 'preview')
- `previewFile` (String, default: '') - Path of currently previewed file

## Public Methods

### `switchToCode(event?: Event): void`
Switches to code editor tab.

### `switchToPreview(event?: Event): void`
Switches to preview tab (only if a file is being previewed).

### `openFile(event: Event): Promise<void>`
Opens a file from the workspace in the preview tab.

**Parameters:**
- `event.currentTarget.dataset.filePath` - Path relative to `/workspace/`

Special case: If filePath is `/code.php`, switches to code tab instead of opening preview.

### `openFileFromDropdown(event: Event): void`
Opens a file selected from the mobile dropdown. Resets dropdown selection after opening.

### `closePreview(event?: Event): void`
Closes the preview tab and switches to code tab.

### `downloadPreviewFile(event?: Event): void`
Downloads the currently previewed file to the user's computer.

## Syntax Highlighting

Uses CodeMirror 6 for read-only preview with syntax highlighting:
- `.php`, `.phar` - PHP syntax
- `.json` - JSON syntax
- `.xml` - XML syntax
- Other extensions - Plain text

## File: `playground_tabs_controller.js`
