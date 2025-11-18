import { Controller } from "@hotwired/stimulus"
import Prism from 'prismjs'
import 'prismjs/components/prism-markup-templating.min.js'
import 'prismjs/components/prism-php.min.js'
import 'prismjs/components/prism-json.min.js'
import 'prismjs/components/prism-csv.min.js'

export default class extends Controller {
    static targets = ["runButton", "formatButton", "uploadButton", "fileInput", "output", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer", "storageIndicator", "fileBrowser", "fileBrowserContent", "filePreviewContainer", "filePreviewTitle", "filePreviewContent"]
    static outlets = ["code-editor", "playground-storage"]
    static values = {
        packageIcon: String,
        linkIcon: String,
        folderIcon: String,
        fileIcon: String
    }

    #currentPreviewFile = null
    #uploadedDatasets = []

    #allowedExtensions = ['csv', 'json', 'xml', 'php', 'phar']

    connect() {
        // Content is hidden by CSS initially
        this.#log('Connecting editor controller')
    }

    onNotification(event) {
        const { message, type, link } = event.detail

        if (link) {
            this.#addNotification(message, type, link)
        } else {
            this.#addNotification(message, type)
        }
    }

    onStorageLoaded(event) {
        this.#log('Code loaded from local storage')
        this.#showIndicator('storage')
    }

    onUrlLoaded(event) {
        this.#log('Code loaded from URL')
        this.#showIndicator('url')
    }

    onDatasetsLoaded(event) {
        this.#log('Datasets loaded from R2')

        // Update file browser when WASM is ready
        const wasmController = this.#getWasmController()
        if (wasmController && wasmController.isReady()) {
            this.#updateFileBrowser()
        } else {
            // WASM not ready yet, will update when resources finish loading
            this.#log('WASM not ready, file browser will update when WASM loads')
        }
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
            url: 'Loaded from snippet'
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

        // Check if WASM is ready before listing files
        if (!wasmController.isReady()) {
            this.#log('WASM not ready yet, skipping file browser update')
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
                    <li class="file-tree-item file clickable" style="padding-left: ${indent}px"
                        data-action="click->playground#previewFile"
                        data-file-path="${entry.path}">
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
        if (errorInfo && this.hasCodeEditorOutlet) {
            this.#log('Highlighting error:', errorInfo)
            this.codeEditorOutlet.highlightError(errorInfo)
        } else {
            this.#log('No errorInfo or code editor outlet not available', {
                hasErrorInfo: !!errorInfo,
                hasOutlet: this.hasCodeEditorOutlet
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
        if (!this.hasCodeEditorOutlet) {
            this.#log('Code editor outlet not connected')
            this.#showOutput('Code editor not found')
            return
        }

        // Clear previous errors before running
        this.codeEditorOutlet.clearErrors()

        const code = this.codeEditorOutlet.getCode()
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

        if (!this.hasCodeEditorOutlet) {
            this.#log('Code editor outlet not connected')
            this.#showOutput('Code editor not found')
            return
        }

        const code = this.codeEditorOutlet.getCode()
        this.#showOutput('Formatting code...')

        if (this.hasFormatButtonTarget) {
            this.formatButtonTarget.disabled = true
        }

        wasmController.formatCode(code, (formattedCode, error, appliedFixers) => {
            if (this.hasFormatButtonTarget) {
                this.formatButtonTarget.disabled = false
            }

            if (error) {
                this.#showOutput('Format error: ' + error)
                return
            }

            this.codeEditorOutlet.setValue(formattedCode)
            this.#showOutput('Code formatted successfully!')

            if (appliedFixers && appliedFixers.length > 0) {
                this.#addNotification(`Applied fixers: ${appliedFixers.join(', ')}`, 'info')
            } else {
                this.#addNotification('No formatting changes needed', 'info')
            }

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

        // Check current uploaded file count
        const currentFiles = this.#getUploadedFileCount(wasmController)

        let uploadedCount = 0
        let skippedCount = 0
        const invalidFiles = []
        const uploadedFiles = []
        const fileSizeErrors = []
        const fileCountErrors = []

        for (const file of files) {
            // Check file count limit (max 3 files in /workspace/uploads/)
            if (currentFiles + uploadedCount >= 3) {
                fileCountErrors.push(file.name)
                skippedCount++
                continue
            }

            // Validate file size (max 2MB)
            const validation = this.#validateFileUpload(file)
            if (!validation.valid) {
                fileSizeErrors.push(`${file.name} (${validation.error})`)
                skippedCount++
                continue
            }

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
                    uploadedFiles.push(`uploads/${file.name}`)
                    this.#log(`Uploaded: ${file.name}`)

                    // Track uploaded dataset for sharing (Task 11)
                    this.#uploadedDatasets.push({
                        path: `/workspace/uploads/${file.name}`,
                        name: file.name,
                        size: file.size
                    })
                } else {
                    skippedCount++
                    this.#log(`Failed to upload: ${file.name}`)
                }
            } catch (error) {
                this.#log(`Error uploading ${file.name}:`, error)
                skippedCount++
            }
        }

        let summaryMessage = `Upload complete: ${uploadedCount} file(s) uploaded, ${skippedCount} skipped.`
        if (invalidFiles.length > 0) {
            summaryMessage += `\nInvalid files (allowed: ${this.#allowedExtensions.join(', ')}): ${invalidFiles.join(', ')}`
        }
        if (fileSizeErrors.length > 0) {
            summaryMessage += `\nFiles too large (max 2MB): ${fileSizeErrors.join(', ')}`
        }
        if (fileCountErrors.length > 0) {
            summaryMessage += `\nMaximum 3 files allowed. Remove existing files before uploading: ${fileCountErrors.join(', ')}`
        }
        this.#showOutput(summaryMessage)

        if (uploadedFiles.length > 0) {
            for (const filePath of uploadedFiles) {
                this.#addNotification(`File uploaded: ${filePath}`, 'success')
            }
        }

        this.#updateFileBrowser()

        if (this.hasFileInputTarget) {
            this.fileInputTarget.value = ''
        }
    }

    /**
     * Get all uploaded datasets for sharing (Task 11)
     * Returns all uploaded datasets (already validated - max 3 files, 2MB each)
     * @returns {Array} - Array of dataset objects: { path, name, size }
     */
    getSelectedDatasets() {
        // Return all uploaded datasets (no selection UI - share all uploaded files)
        return this.#uploadedDatasets.slice(0, 3) // Safety: max 3
    }

    /**
     * Validate file before uploading
     * @param {File} file - File to validate
     * @returns {object} - { valid: boolean, error?: string }
     */
    #validateFileUpload(file) {
        // Check file size (2MB max)
        const MAX_SIZE = 2 * 1024 * 1024 // 2MB
        if (file.size > MAX_SIZE) {
            return {
                valid: false,
                error: `${(file.size / 1024 / 1024).toFixed(2)}MB`
            }
        }

        return { valid: true }
    }

    /**
     * Get count of files in /workspace/uploads/
     * @param {object} wasmController - WASM controller instance
     * @returns {number} - Number of files currently in uploads directory
     */
    #getUploadedFileCount(wasmController) {
        try {
            const files = wasmController.listFiles('/workspace/uploads')
            return files.filter(f => f.type === 'file').length
        } catch (error) {
            this.#log('Error counting uploaded files:', error)
            return 0
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

    #addNotification(message, type = 'info', additionalContent = null) {
        if (!this.hasOutputTarget) {
            return
        }

        const timestamp = new Date().toLocaleTimeString()
        const prefix = {
            info: '[INFO]',
            success: '[SUCCESS]',
            warning: '[WARNING]',
            error: '[ERROR]'
        }[type] || '[INFO]'

        let notification = `${timestamp} ${prefix} ${message}`

        if (additionalContent) {
            notification += '\n\n' + additionalContent
        }

        this.outputTarget.textContent = notification
    }

    previewFile(event) {
        const filePath = event.currentTarget.dataset.filePath
        if (!filePath) {
            this.#log('No file path found')
            return
        }

        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        try {
            // Read file content from WASM filesystem
            const fullPath = `/workspace${filePath}`
            this.#log('Reading file:', fullPath)

            const content = wasmController.readFile(fullPath)

            if (content === null || content === undefined) {
                this.#addNotification(`Failed to read file: ${filePath}`, 'error')
                return
            }

            // Store current preview file for download
            this.#currentPreviewFile = { path: filePath, content: content }

            // Update preview title
            if (this.hasFilePreviewTitleTarget) {
                this.filePreviewTitleTarget.textContent = `File Preview: ${filePath}`
            }

            // Update preview content with syntax highlighting
            if (this.hasFilePreviewContentTarget) {
                // Get file extension for language detection
                const extension = filePath.split('.').pop().toLowerCase()
                const languageMap = {
                    'php': 'php',
                    'json': 'json',
                    'csv': 'csv',
                    'xml': 'markup',
                    'phar': 'php'
                }

                const language = languageMap[extension] || 'none'

                // Set content and language class
                this.filePreviewContentTarget.textContent = content
                this.filePreviewContentTarget.className = `language-${language}`

                // Apply syntax highlighting
                Prism.highlightElement(this.filePreviewContentTarget)
            }

            // Show preview container
            if (this.hasFilePreviewContainerTarget) {
                this.filePreviewContainerTarget.style.display = 'block'

                // Scroll to preview smoothly
                this.filePreviewContainerTarget.scrollIntoView({ behavior: 'smooth', block: 'start' })
            }

            this.#log('File preview loaded:', filePath)
        } catch (error) {
            this.#log('Error reading file:', error)
            this.#addNotification(`Error reading file: ${error.message}`, 'error')
        }
    }

    closeFilePreview(event) {
        if (event) {
            event.preventDefault()
        }

        if (this.hasFilePreviewContainerTarget) {
            this.filePreviewContainerTarget.style.display = 'none'
        }

        this.#currentPreviewFile = null
    }

    downloadPreviewFile(event) {
        if (event) {
            event.preventDefault()
        }

        if (!this.#currentPreviewFile) {
            this.#log('No file to download')
            return
        }

        try {
            const { path, content } = this.#currentPreviewFile
            const fileName = path.split('/').pop()

            // Create blob and download
            const blob = new Blob([content], { type: 'text/plain' })
            const url = URL.createObjectURL(blob)
            const a = document.createElement('a')
            a.href = url
            a.download = fileName
            document.body.appendChild(a)
            a.click()
            document.body.removeChild(a)
            URL.revokeObjectURL(url)

            this.#addNotification(`Downloaded: ${fileName}`, 'success')
        } catch (error) {
            this.#log('Error downloading file:', error)
            this.#addNotification(`Error downloading file: ${error.message}`, 'error')
        }
    }

    #log(...args) {
        if (this.application.debug) {
            console.log('[Editor]', ...args)
        }
    }
}
