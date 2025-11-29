import { Controller } from "@hotwired/stimulus"
import { EditorView, basicSetup } from "codemirror"
import { EditorState } from "@codemirror/state"
import { php } from "@codemirror/lang-php"
import { json } from "@codemirror/lang-json"
import { xml } from "@codemirror/lang-xml"
import { flowThemeExtension } from "../codemirror/themes/theme-flow.js"

export default class extends Controller {
    static outlets = ["wasm", "code-editor"]
    static targets = ["tabBar", "codeTab", "previewTab", "previewTabName", "codePanel", "previewPanel", "downloadBtn"]
    static values = {
        activeTab: { type: String, default: 'code' },
        previewFile: { type: String, default: '' }
    }

    #previewEditor = null
    #debug = false

    connect() {
        this.#debug = this.application.debug
        this.#log('Connecting tabs controller')
    }

    disconnect() {
        if (this.#previewEditor) {
            this.#previewEditor.destroy()
            this.#previewEditor = null
        }
    }

    switchToCode(event) {
        if (event) {
            event.preventDefault()
            event.stopPropagation()
        }

        this.activeTabValue = 'code'
        this.#updateTabUI()
    }

    switchToPreview(event) {
        if (event) {
            event.preventDefault()
            event.stopPropagation()
        }

        if (!this.previewFileValue) {
            return
        }

        this.activeTabValue = 'preview'
        this.#updateTabUI()
    }

    async openFile(event) {
        const filePath = event.currentTarget.dataset.filePath
        if (!filePath) {
            this.#log('No file path found')
            return
        }

        if (filePath === '/code.php') {
            this.switchToCode()
            return
        }

        if (!this.hasWasmOutlet) {
            this.#log('WASM outlet not found')
            return
        }

        this.#log('Opening file:', filePath)

        try {
            const fullPath = `/workspace${filePath}`
            const result = await this.wasmOutlet.readFile(fullPath)

            if (!result.success || result.content === null || result.content === undefined) {
                this.#log('Failed to read file:', filePath)
                return
            }

            const content = typeof result.content === 'string'
                ? result.content
                : new TextDecoder().decode(result.content)

            this.previewFileValue = filePath
            this.#setPreviewContent(content, filePath)
            this.activeTabValue = 'preview'
            this.#updateTabUI()

            this.#log('File opened:', filePath)
        } catch (error) {
            this.#log('Error opening file:', error)
        }
    }

    closePreview(event) {
        if (event) {
            event.preventDefault()
            event.stopPropagation()
        }

        this.previewFileValue = ''
        this.activeTabValue = 'code'
        this.#updateTabUI()
    }

    openFileFromDropdown(event) {
        const filePath = event.target.value
        if (!filePath) {
            return
        }

        event.target.value = ''

        this.openFile({ currentTarget: { dataset: { filePath } } })
    }

    downloadPreviewFile(event) {
        if (event) {
            event.preventDefault()
        }

        if (!this.previewFileValue || !this.#previewEditor) {
            this.#log('No file to download')
            return
        }

        try {
            const content = this.#previewEditor.state.doc.toString()
            const fileName = this.previewFileValue.split('/').pop()

            const blob = new Blob([content], { type: 'text/plain' })
            const url = URL.createObjectURL(blob)
            const a = document.createElement('a')
            a.href = url
            a.download = fileName
            document.body.appendChild(a)
            a.click()
            document.body.removeChild(a)
            URL.revokeObjectURL(url)

            this.#log('Downloaded:', fileName)
        } catch (error) {
            this.#log('Error downloading file:', error)
        }
    }

    #setPreviewContent(content, filePath) {
        if (!this.hasPreviewPanelTarget) {
            return
        }

        const extension = filePath.split('.').pop().toLowerCase()
        const languageExtension = this.#getLanguageExtension(extension)

        if (this.#previewEditor) {
            this.#previewEditor.destroy()
            this.#previewEditor = null
            this.previewPanelTarget.innerHTML = ''
        }

        this.#createPreviewEditor(content, languageExtension)

        if (this.hasPreviewTabNameTarget) {
            this.previewTabNameTarget.textContent = filePath.split('/').pop()
        }
    }

    #createPreviewEditor(content, languageExtension) {
        const extensions = [
            basicSetup,
            languageExtension || [],
            flowThemeExtension,
            EditorState.readOnly.of(true),
            EditorView.editable.of(false)
        ].flat()

        this.#previewEditor = new EditorView({
            state: EditorState.create({
                doc: content,
                extensions: extensions
            }),
            parent: this.previewPanelTarget
        })
    }

    #getLanguageExtension(extension) {
        const languageMap = {
            'php': php(),
            'phar': php(),
            'json': json(),
            'xml': xml()
        }

        return languageMap[extension] || null
    }

    #updateTabUI() {
        const isCode = this.activeTabValue === 'code'
        const hasPreview = !!this.previewFileValue

        if (this.hasCodeTabTarget) {
            this.codeTabTarget.classList.toggle('active', isCode)
        }

        if (this.hasPreviewTabTarget) {
            this.previewTabTarget.classList.toggle('active', !isCode && hasPreview)
            this.previewTabTarget.style.display = hasPreview ? 'flex' : 'none'
        }

        if (this.hasDownloadBtnTarget) {
            this.downloadBtnTarget.style.display = hasPreview && !isCode ? 'block' : 'none'
        }

        if (this.hasCodePanelTarget) {
            this.codePanelTarget.style.display = isCode ? 'block' : 'none'
        }

        if (this.hasPreviewPanelTarget) {
            this.previewPanelTarget.style.display = isCode ? 'none' : 'block'
        }
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[PlaygroundTabs]', ...args)
        }
    }
}
