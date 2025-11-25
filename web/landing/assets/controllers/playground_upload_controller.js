import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["wasm", "playground-output"]
    static targets = ["fileInput"]
    static values = {
        maxFileSize: { type: Number, default: 2097152 },
        maxFileCount: { type: Number, default: 3 },
        allowedExtensions: { type: Array, default: ['csv', 'json', 'xml', 'php', 'phar'] }
    }

    triggerUpload(event) {
        event.preventDefault()

        if (!this.hasFileInputTarget) {
            return
        }

        this.fileInputTarget.click()
    }

    async uploadFile(file) {
        await Promise.all([
            this.wasmOutlet.onLoad()
        ])

        const validation = this.#validate(file)
        if (!validation.valid) {
            this.playgroundOutputOutlet.show({ content: validation.error, type: 'error' })
            return
        }

        const arrayBuffer = await file.arrayBuffer()
        const uint8Array = new Uint8Array(arrayBuffer)

        const result = await this.wasmOutlet.writeFile(
            `/workspace/uploads/${file.name}`,
            uint8Array
        )

        if (result.success) {
            this.playgroundOutputOutlet.show({ content: `File uploaded: ${file.name}`, type: 'success' })
            this.dispatch('file-uploaded', { detail: { filename: file.name, size: file.size }, bubbles: true })
        }
    }

    async handleFileUpload(event) {
        this.dispatch('action-started', { bubbles: true })

        try {
            const files = Array.from(event.target.files)
            for (const file of files) {
                await this.uploadFile(file)
            }
            if (this.hasFileInputTarget) {
                this.fileInputTarget.value = ''
            }
        } finally {
            this.dispatch('action-finished', { bubbles: true })
        }
    }

    async listFiles() {
        if (!this.hasWasmOutlet) {
            return []
        }
        const result = await this.wasmOutlet.listFiles('/workspace/uploads')
        return result.files || []
    }

    #validateFilename(filename) {
        if (!filename || filename.length === 0) {
            return { valid: false, error: 'Filename cannot be empty' }
        }

        if (filename.length > 255) {
            return { valid: false, error: 'Filename too long (max 255 characters)' }
        }

        if (filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
            return { valid: false, error: 'Filename cannot contain path separators' }
        }

        const safePattern = /^[a-zA-Z0-9._\-() ]+$/
        if (!safePattern.test(filename)) {
            return { valid: false, error: 'Filename contains invalid characters. Only letters, numbers, spaces, dash, underscore, dot, and parentheses are allowed' }
        }

        if (!filename.includes('.')) {
            return { valid: false, error: 'Filename must have an extension' }
        }

        return { valid: true }
    }

    #validate(file) {
        const filenameCheck = this.#validateFilename(file.name)
        if (!filenameCheck.valid) {
            return filenameCheck
        }

        if (file.size > this.maxFileSizeValue) {
            return { valid: false, error: `File too large: ${file.name} (max 2MB)` }
        }

        const ext = file.name.split('.').pop().toLowerCase()
        if (!this.allowedExtensionsValue.includes(ext)) {
            return { valid: false, error: `Invalid extension: ${ext}` }
        }

        return { valid: true }
    }
}
