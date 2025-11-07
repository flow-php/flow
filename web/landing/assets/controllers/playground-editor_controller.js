import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["runButton", "output", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer", "storageIndicator", "fileBrowser", "fileBrowserContent"]
    static outlets = ["code-mirror-editor", "playground-storage"]

    connect() {
        // Content is hidden by CSS initially
        this.#log('Connecting editor controller')
    }

    onStorageLoaded(event) {
        this.#log('Code loaded from local storage')
        this.#showIndicator('storage')
    }

    onUrlLoaded(event) {
        this.#log('Code loaded from URL')
        this.#showIndicator('url')
    }

    #showIndicator(source) {
        if (!this.hasStorageIndicatorTarget) {
            return
        }

        const icons = {
            storage: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
                <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
                <line x1="12" y1="22.08" x2="12" y2="12"></line>
            </svg>`,
            url: `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"></path>
                <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"></path>
            </svg>`
        }

        const labels = {
            storage: 'Loaded from local storage',
            url: 'Loaded from shared link'
        }

        this.storageIndicatorTarget.innerHTML = icons[source] + '<span>' + labels[source] + '</span>'
        this.storageIndicatorTarget.style.display = 'flex'
    }

    // Called when WASM is ready via event
    onWasmReady(event) {
        this.#log('WASM ready, loading resources')
        this.#loadResources()
    }

    // Called when WASM resources are loaded via event
    onWasmResourcesLoaded(event) {
        this.#log('WASM resources loaded successfully')
        this.#hideLoading()
        this.#showOutput('Click "Run" to execute your code.')
        this.#updateFileBrowser()
    }

    #updateFileBrowser() {
        if (!this.hasFileBrowserContentTarget) {
            return
        }

        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        const files = wasmController.listFiles('/')
        this.#log('Files in WASM filesystem:', files)

        // Build file tree structure
        const tree = this.#buildFileTree(files)

        // Render file tree
        this.fileBrowserContentTarget.innerHTML = this.#renderFileTree(tree)
    }

    #buildFileTree(files) {
        const tree = {}

        for (const file of files) {
            const parts = file.path.split('/').filter(p => p)
            let current = tree

            for (let i = 0; i < parts.length; i++) {
                const part = parts[i]
                const isLast = i === parts.length - 1

                if (!current[part]) {
                    current[part] = {
                        name: part,
                        path: file.path,
                        type: file.type,
                        children: {}
                    }
                }

                if (!isLast && file.type === 'directory') {
                    current = current[part].children
                }
            }
        }

        return tree
    }

    #renderFileTree(tree, level = 0) {
        let html = '<ul class="file-tree">'

        const entries = Object.values(tree).sort((a, b) => {
            // Directories first, then files
            if (a.type === 'directory' && b.type !== 'directory') return -1
            if (a.type !== 'directory' && b.type === 'directory') return 1
            return a.name.localeCompare(b.name)
        })

        for (const entry of entries) {
            const indent = level * 16

            if (entry.type === 'directory') {
                html += `
                    <li class="file-tree-item directory" style="padding-left: ${indent}px">
                        <svg class="icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"></path>
                        </svg>
                        <span>${entry.name}</span>
                    </li>
                `

                // Render children
                if (entry.children && Object.keys(entry.children).length > 0) {
                    html += this.#renderFileTree(entry.children, level + 1)
                }
            } else {
                html += `
                    <li class="file-tree-item file" style="padding-left: ${indent}px">
                        <svg class="icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"></path>
                            <polyline points="13 2 13 9 20 9"></polyline>
                        </svg>
                        <span>${entry.name}</span>
                    </li>
                `
            }
        }

        html += '</ul>'
        return html
    }

    #loadResources() {
        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        this.#showOutput('Loading Flow PHP library...')
        wasmController.loadResources()
    }

    // Called when WASM reports progress
    onWasmProgress(event) {
        const { message, percent } = event.detail
        this.#showLoading(message, percent)
    }

    // Called when WASM has output
    onWasmOutput(event) {
        const { output } = event.detail
        this.#showOutput(output)
    }

    // Called when WASM has an error
    onWasmError(event) {
        const { error, errorInfo } = event.detail
        this.#log('Error event received:', { error, errorInfo })
        this.#hideLoading()
        this.#showContentAfterLoading()
        this.#showOutput(error)

        // Highlight error in code editor if we have errorInfo
        if (errorInfo && this.hasCodeMirrorEditorOutlet) {
            this.#log('Highlighting error:', errorInfo)
            this.codeMirrorEditorOutlet.highlightError(errorInfo)
        } else {
            this.#log('No errorInfo or code editor outlet not available', {
                hasErrorInfo: !!errorInfo,
                hasOutlet: this.hasCodeMirrorEditorOutlet
            })
        }
    }

    // Run button click handler
    run(event) {
        event.preventDefault()

        // Get WASM controller from same element
        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        // Check if WASM is ready
        if (!wasmController.isReady()) {
            this.#showOutput('PHP module not loaded yet, please wait...')
            return
        }

        // Get code from code-editor outlet
        if (!this.hasCodeMirrorEditorOutlet) {
            this.#log('Code editor outlet not connected')
            this.#showOutput('Code editor not found')
            return
        }

        // Clear previous errors before running
        this.codeMirrorEditorOutlet.clearErrors()

        const code = this.codeMirrorEditorOutlet.getCode()
        this.#showOutput('Running...')

        // Save code to local storage
        if (this.hasPlaygroundStorageOutlet) {
            this.playgroundStorageOutlet.saveCode()
        }

        // Execute code via WASM controller
        wasmController.evaluate(code)
    }

    #getWasmController() {
        return this.application.getControllerForElementAndIdentifier(
            this.element,
            'wasm'
        )
    }

    #showLoading(message, percent) {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = message
        }
        if (this.hasLoadingPercentTarget && percent !== undefined) {
            this.loadingPercentTarget.textContent = Math.round(percent) + '%'
        }
        if (this.hasLoadingBarTarget && percent !== undefined) {
            this.loadingBarTarget.style.width = percent + '%'
        }
        if (this.hasRunButtonTarget) {
            this.runButtonTarget.disabled = true
        }
    }

    #hideLoading() {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = ''
        }
        if (this.hasLoadingPercentTarget) {
            this.loadingPercentTarget.textContent = ''
        }
        if (this.hasLoadingBarTarget) {
            this.loadingBarTarget.style.width = '0%'
        }
        if (this.hasRunButtonTarget) {
            this.runButtonTarget.disabled = false
        }
        this.#showContentAfterLoading()
    }

    #showContentAfterLoading() {
        if (this.hasNavigationTarget) {
            this.navigationTarget.style.display = 'grid'
        }
        if (this.hasEditorTarget) {
            this.editorTarget.style.display = 'grid'
        }
        if (this.hasOutputContainerTarget) {
            this.outputContainerTarget.style.display = 'block'
        }
    }

    #showOutput(message) {
        if (this.hasOutputTarget) {
            this.outputTarget.textContent = message
        }
    }

    #log(...args) {
        if (this.application.debug) {
            console.log('[Editor]', ...args)
        }
    }
}
