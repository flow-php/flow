# Playground Architecture

The Flow PHP Playground is an interactive code editor that runs PHP code directly in the browser using WebAssembly. It
enables users to experiment with Flow PHP's ETL capabilities without requiring a server-side PHP installation.

## Core Architecture

The playground is built using Stimulus controllers that communicate via custom events. It consists of three layers:

1. **Core Infrastructure** - WASM runtime, code editor, security
2. **Playground Controllers** - Feature-specific controllers for playground functionality
3. **UI Controllers** - Output display, workspace management, help system

## Core Infrastructure Controllers

### code-editor (code_editor_controller.js)

**Purpose:** CodeMirror-based PHP editor with syntax highlighting and error display

**Responsibilities:**

- PHP syntax highlighting and autocompletion
- Flow PHP DSL completions (DataFrame, scalar functions, DSL helpers)
- Error highlighting with line/column navigation
- Code change detection and synchronization

**See:** [playground/code-editor.md](playground/code-editor.md)

---

### wasm (wasm_controller.js)

**Purpose:** PHP WebAssembly runtime for executing PHP code in the browser

**Responsibilities:**

- Load and initialize PHP WASM module
- Manage virtual filesystem (`/workspace/`, `/workspace/uploads/`)
- Execute PHP code and capture output/errors
- Load playground resources (PHAR files, datasets)
- Format PHP code using CS-Fixer

**See:** [playground/wasm.md](playground/wasm.md)

---

### turnstile (turnstile_controller.js)

**Purpose:** Cloudflare Turnstile CAPTCHA integration for bot protection

**Responsibilities:**

- Initialize Turnstile widget
- Obtain verification tokens for API requests
- Handle test/bypass modes for development

**See:** [playground/turnstile.md](playground/turnstile.md)

## Playground Feature Controllers

### playground (playground_controller.js)

**Purpose:** Main orchestrator and event hub for the playground

**Responsibilities:**

- Coordinate loading of all outlets (wasm, code-editor, turnstile, output)
- Display loading progress during WASM initialization
- Handle file preview from workspace
- Show storage/URL indicators
- Route events between controllers

**See:** [playground/playground.md](playground/playground.md)

---

### playground-run (playground_run_controller.js)

**Purpose:** Execute PHP code in the WASM runtime

**Responsibilities:**

- Trigger code execution
- Manage button states during execution
- Handle execution results (success/error)
- Dispatch execution events with timing data

**See:** [playground/playground-run.md](playground/playground-run.md)

---

### playground-format (playground_format_controller.js)

**Purpose:** Format PHP code using CS-Fixer via WASM

**Responsibilities:**

- Format code on demand
- Display applied fixers
- Handle formatting errors

**See:** [playground/playground-format.md](playground/playground-format.md)

---

### playground-reset (playground_reset_controller.js)

**Purpose:** Reset playground to default state

**Responsibilities:**

- Confirm reset action
- Clear local storage
- Trigger page reload

**See:** [playground/playground-reset.md](playground/playground-reset.md)

---

### playground-storage (playground_storage_controller.js)

**Purpose:** Persist code to browser localStorage

**Responsibilities:**

- Auto-save code changes (debounced)
- Load code from localStorage on connect
- Prevent loading when shared snippet is present
- Clear storage on reset

**See:** [playground/playground-storage.md](playground/playground-storage.md)

---

### playground-share (playground_share_controller.js)

**Purpose:** Share code snippets via Cloudflare R2

**Responsibilities:**

- Upload code and datasets to API
- Generate shareable URLs
- Load shared snippets from R2
- Handle snippet expiration
- Copy share links to clipboard

**See:** [playground/playground-share.md](playground/playground-share.md)

---

### playground-upload (playground_upload_controller.js)

**Purpose:** Upload datasets to WASM filesystem

**Responsibilities:**

- Validate file uploads (size, extension, filename)
- Write files to `/workspace/uploads/`
- List uploaded files
- Trigger file-uploaded events

**See:** [playground/playground-upload.md](playground/playground-upload.md)

---

### playground-workspace (playground_workspace_controller.js)

**Purpose:** Display and navigate the WASM virtual filesystem

**Responsibilities:**

- Render file tree from `/workspace/`
- Listen for file creation/deletion/update events
- Refresh tree on changes
- Track file selection state

**See:** [playground/playground-workspace.md](playground/playground-workspace.md)

---

### playground-output (playground_output_controller.js)

**Purpose:** Display execution output and messages

**Responsibilities:**

- Render timestamped messages with type indicators
- Support multiple message types (info, success, warning, error)
- Append additional messages
- Clear output on demand

**See:** [playground/playground-output.md](playground/playground-output.md)

---

### playground-help (playground_help_controller.js)

**Purpose:** Display contextual help and documentation

**Responsibilities:**

- Show/hide help section
- Display specific help topics
- Manage help section visibility

**See:** [playground/playground-help.md](playground/playground-help.md)

## Event Flow

The controllers communicate via Stimulus events. Key event flows:

### Code Execution Flow

```
user clicks "Run"
→ playground-run:run()
→ code-editor:getCode()
→ wasm:run(code)
→ wasm:output OR wasm:error
→ playground-output:show()
→ playground-run:executed event
```

### Code Sharing Flow

```
user clicks "Share"
→ playground-share:share()
→ turnstile:getToken()
→ playground-upload:listFiles()
→ wasm:readFile() for datasets
→ API POST with Turnstile token
→ Update browser URL with ?snippet={id}
→ playground:notification event
```

### Snippet Loading Flow

```
page load with ?snippet={id}
→ playground-share:loadCodeFromUrl()
→ fetch snippet.json from R2
→ fetch code.php from R2
→ code-editor:setCode()
→ fetch datasets from R2
→ wasm:writeFile() for each dataset
→ playground:loaded-from-url event
```

### File Upload Flow

```
user selects file
→ playground-upload:handleFileUpload()
→ validate file
→ wasm:writeFile(/workspace/uploads/{name})
→ wasm:file-created event
→ playground-workspace:refreshTree()
```

### Code Storage Flow

```
code-editor:code-changed event
→ playground-storage:save() (debounced)
→ localStorage.setItem()
→ playground-storage:saved event
```

## File Locations

- **Controllers:** `web/landing/assets/controllers/*_controller.js`
- **Templates:** `web/landing/templates/playground/index.html.twig`
- **API:** `terraform/cloudflare/workers/snippet-upload.js` (Cloudflare Worker)
- **Tests:** `web/landing/tests/Flow/Website/Tests/Functional/*PlaygroundTest.php`

## External Dependencies

- **CodeMirror 6** - Code editor with PHP support
- **PHP WASM** - PHP 8.3 compiled to WebAssembly
- **Cloudflare Turnstile** - Bot protection
- **Cloudflare R2** - Snippet storage (S3-compatible)
- **Stimulus** - JavaScript framework

## Development Notes

- All controllers follow Stimulus conventions (outlets, targets, values)
- Events use kebab-case naming (`code-editor:code-changed`)
- Controllers use private fields (prefixed with `#`) for encapsulation
- Debug logging controlled by `application.debug` flag
- WASM filesystem is ephemeral (resets on page reload)
- Snippets expire after 90 days (configurable in Worker)
- Code is stored in `/workspace/code.php` and synced before run/format/share operations
- Default code loaded as WASM resource from `web/landing/assets/wasm/code.php.wasm`
