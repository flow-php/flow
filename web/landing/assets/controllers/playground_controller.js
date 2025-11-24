import { Controller } from "@hotwired/stimulus"
import Prism from 'prismjs'
import 'prismjs/components/prism-markup-templating.min.js'
import 'prismjs/components/prism-php.min.js'
import 'prismjs/components/prism-json.min.js'
import 'prismjs/components/prism-csv.min.js'

export default class extends Controller {
    static outlets = ["wasm", "code-editor", "turnstile", "playground-output"]
    static targets = ["loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer", "storageIndicator", "filePreviewContainer", "filePreviewTitle", "filePreviewContent"]
    static values = {
        packageIcon: String,
        linkIcon: String
    }

    #currentPreviewFile = null

    connect() {
        this.#log('Connecting playground controller')
    }

    onNotification(event) {
        const { message, type } = event.detail
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: message, type })
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
    }

    onWasmInitialized(event) {
        this.#log('WASM initialized successfully')
        this.hideLoading()
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: 'Click "Run" to execute your code.', type: 'info' })
        }
    }

    onWasmLoadingProgress(event) {
        const { message, percent } = event.detail
        this.showLoading(message, percent)
    }

    onWasmOutput(event) {
        const { output } = event.detail
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: output, type: 'success' })
        }
    }

    onWasmError(event) {
        const { error, errorInfo } = event.detail
        this.#log('Error event received:', { error, errorInfo })
        this.hideLoading()
        this.#showContentAfterLoading()
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: error, type: 'error' })
        }

        if (errorInfo && this.hasCodeEditorOutlet) {
            this.#log('Highlighting error:', errorInfo)
            this.codeEditorOutlet.highlightError(errorInfo)
        }
    }

    showLoading(message, percent) {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = message
        }
        if (this.hasLoadingPercentTarget && percent !== undefined) {
            this.loadingPercentTarget.textContent = Math.round(percent) + '%'
        }
        if (this.hasLoadingBarTarget && percent !== undefined) {
            this.loadingBarTarget.style.width = percent + '%'
        }
    }

    hideLoading() {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = ''
        }
        if (this.hasLoadingPercentTarget) {
            this.loadingPercentTarget.textContent = ''
        }
        if (this.hasLoadingBarTarget) {
            this.loadingBarTarget.style.width = '0%'
        }
        this.#showContentAfterLoading()
    }

    isLoaded() {
        return (
            this.hasWasmOutlet && this.wasmOutlet.isLoaded() &&
            this.hasCodeEditorOutlet && this.codeEditorOutlet.isLoaded() &&
            this.hasTurnstileOutlet && this.turnstileOutlet.isLoaded() &&
            this.hasPlaygroundOutputOutlet && this.playgroundOutputOutlet.isLoaded()
        )
    }

    async onLoad() {
        const promises = []

        if (this.hasWasmOutlet) promises.push(this.wasmOutlet.onLoad())
        if (this.hasCodeEditorOutlet) promises.push(this.codeEditorOutlet.onLoad())
        if (this.hasTurnstileOutlet) promises.push(this.turnstileOutlet.onLoad())
        if (this.hasPlaygroundOutputOutlet) promises.push(this.playgroundOutputOutlet.onLoad())

        await Promise.all(promises)
    }

    async previewFile(event) {
        const filePath = event.currentTarget.dataset.filePath
        if (!filePath) {
            this.#log('No file path found')
            return
        }

        if (!this.hasWasmOutlet) {
            this.#log('WASM outlet not found')
            return
        }

        try {
            const fullPath = `/workspace${filePath}`
            this.#log('Reading file:', fullPath)

            const result = await this.wasmOutlet.readFile(fullPath)

            if (!result.success || !result.content) {
                if (this.hasPlaygroundOutputOutlet) {
                    this.playgroundOutputOutlet.show({ content: `Failed to read file: ${filePath}`, type: 'error' })
                }
                return
            }

            const content = result.content

            this.#currentPreviewFile = { path: filePath, content: content }

            if (this.hasFilePreviewTitleTarget) {
                this.filePreviewTitleTarget.textContent = `File Preview: ${filePath}`
            }

            if (this.hasFilePreviewContentTarget) {
                const extension = filePath.split('.').pop().toLowerCase()
                const languageMap = {
                    'php': 'php',
                    'json': 'json',
                    'csv': 'csv',
                    'xml': 'markup',
                    'phar': 'php'
                }

                const language = languageMap[extension] || 'none'

                this.filePreviewContentTarget.textContent = content
                this.filePreviewContentTarget.className = `language-${language}`

                Prism.highlightElement(this.filePreviewContentTarget)
            }

            if (this.hasFilePreviewContainerTarget) {
                this.filePreviewContainerTarget.style.display = 'block'
                this.filePreviewContainerTarget.scrollIntoView({ behavior: 'smooth', block: 'start' })
            }

            this.#log('File preview loaded:', filePath)
        } catch (error) {
            this.#log('Error reading file:', error)
            if (this.hasPlaygroundOutputOutlet) {
                this.playgroundOutputOutlet.show({ content: `Error reading file: ${error.message}`, type: 'error' })
            }
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

            const blob = new Blob([content], { type: 'text/plain' })
            const url = URL.createObjectURL(blob)
            const a = document.createElement('a')
            a.href = url
            a.download = fileName
            document.body.appendChild(a)
            a.click()
            document.body.removeChild(a)
            URL.revokeObjectURL(url)

            if (this.hasPlaygroundOutputOutlet) {
                this.playgroundOutputOutlet.show({ content: `Downloaded: ${fileName}`, type: 'success' })
            }
        } catch (error) {
            this.#log('Error downloading file:', error)
            if (this.hasPlaygroundOutputOutlet) {
                this.playgroundOutputOutlet.show({ content: `Error downloading file: ${error.message}`, type: 'error' })
            }
        }
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

    #log(...args) {
        if (this.application.debug) {
            console.log('[Playground]', ...args)
        }
    }
}
