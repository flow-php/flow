import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["runButton", "formatButton", "uploadButton", "fileInput", "output", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer", "storageIndicator", "fileBrowser", "fileBrowserContent"]
    static outlets = ["code-mirror-editor", "playground-storage"]
    static values = {
        packageIcon: String,
        linkIcon: String,
        folderIcon: String,
        fileIcon: String
    }

    #allowedExtensions = ['csv', 'json', 'xml', 'php', 'phar']

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
            storage: `<img src="${this.packageIconValue}" width="16" height="16" alt="">`,
            url: `<img src="${this.linkIconValue}" width="16" height="16" alt="">`
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

        // Only list files from /workspace directory
        const files = wasmController.listFiles('/workspace')
        this.#log('Files in /workspace:', files)

        // Filter to only include files within workspace and strip the /workspace prefix
        const workspaceFiles = files
            .filter(file => file.path.startsWith('/workspace/'))
            .map(file => ({
                ...file,
                path: file.path.substring('/workspace'.length) || '/'
            }))

        // Build file tree structure
        const tree = this.#buildFileTree(workspaceFiles)

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
                    // For the last part, use the file's actual type
                    // For intermediate parts, they must be directories
                    current[part] = {
                        name: part,
                        path: isLast ? file.path : parts.slice(0, i + 1).join('/'),
                        type: isLast ? file.type : 'directory',
                        children: {}
                    }
                }

                // Navigate to children for all parts except the last one
                if (!isLast) {
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
                        <img src="${this.folderIconValue}" class="icon" width="16" height="16" alt="">
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
                        <img src="${this.fileIconValue}" class="icon" width="16" height="16" alt="">
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
        this.#updateFileBrowser()
    }

    // Called when WASM has an error
    onWasmError(event) {
        const { error, errorInfo } = event.detail
        this.#log('Error event received:', { error, errorInfo })
        this.#hideLoading()
        this.#showContentAfterLoading()
        this.#showOutput(error)
        this.#updateFileBrowser()

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

    // Format button click handler
    format(event) {
        event.preventDefault()

        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        if (!wasmController.isReady()) {
            this.#showOutput('PHP module not loaded yet, please wait...')
            return
        }

        if (!this.hasCodeMirrorEditorOutlet) {
            this.#log('Code editor outlet not connected')
            this.#showOutput('Code editor not found')
            return
        }

        const code = this.codeMirrorEditorOutlet.getCode()
        this.#showOutput('Formatting code...')

        if (this.hasFormatButtonTarget) {
            this.formatButtonTarget.disabled = true
        }

        wasmController.formatCode(code, (formattedCode, error) => {
            if (this.hasFormatButtonTarget) {
                this.formatButtonTarget.disabled = false
            }

            if (error) {
                this.#showOutput('Format error: ' + error)
                return
            }

            this.codeMirrorEditorOutlet.setValue(formattedCode)
            this.#showOutput('Code formatted successfully!')

            if (this.hasPlaygroundStorageOutlet) {
                this.playgroundStorageOutlet.saveCode()
            }
        })
    }

    triggerUpload(event) {
        event.preventDefault()

        if (!this.hasFileInputTarget) {
            this.#log('File input not found')
            return
        }

        this.fileInputTarget.click()
    }

    async handleFileUpload(event) {
        const files = event.target.files

        if (!files || files.length === 0) {
            return
        }

        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            this.#showOutput('Error: WASM controller not available')
            return
        }

        if (!wasmController.isReady()) {
            this.#showOutput('PHP module not loaded yet, please wait...')
            return
        }

        let uploadedCount = 0
        let skippedCount = 0
        const invalidFiles = []

        for (const file of files) {
            const extension = file.name.split('.').pop().toLowerCase()

            if (!this.#allowedExtensions.includes(extension)) {
                invalidFiles.push(file.name)
                skippedCount++
                continue
            }

            try {
                const arrayBuffer = await file.arrayBuffer()
                const uint8Array = new Uint8Array(arrayBuffer)

                const success = wasmController.uploadFile(file.name, uint8Array)

                if (success) {
                    uploadedCount++
                    this.#log(`Uploaded: ${file.name}`)
                } else {
                    skippedCount++
                    this.#log(`Failed to upload: ${file.name}`)
                }
            } catch (error) {
                this.#log(`Error uploading ${file.name}:`, error)
                skippedCount++
            }
        }

        if (invalidFiles.length > 0) {
            this.#showOutput(`Upload complete: ${uploadedCount} file(s) uploaded, ${skippedCount} skipped.\nInvalid files (allowed: ${this.#allowedExtensions.join(', ')}): ${invalidFiles.join(', ')}`)
        } else {
            this.#showOutput(`Upload complete: ${uploadedCount} file(s) uploaded, ${skippedCount} skipped.`)
        }

        this.#updateFileBrowser()

        if (this.hasFileInputTarget) {
            this.fileInputTarget.value = ''
        }
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
