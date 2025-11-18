import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["code-editor", "playground", "wasm", "turnstile"]
    static values = {
        apiUrl: String,
        snippetsUrl: String
    }
    #debug = false
    #snippetLoaded = false
    #boundHandleCodeChanged = null

    connect() {
        this.#debug = this.application.debug
        this.#log('API URL:', this.apiUrlValue)
        this.#log('Snippets URL:', this.snippetsUrlValue)

        this.#boundHandleCodeChanged = this.#handleCodeChanged.bind(this)
        this.element.addEventListener('code-changed', this.#boundHandleCodeChanged)
    }

    disconnect() {
        if (this.#boundHandleCodeChanged) {
            this.element.removeEventListener('code-changed', this.#boundHandleCodeChanged)
        }
    }

    codeEditorOutletConnected() {
        this.loadCodeFromUrl()
    }

    /**
     * Load code from URL - check for ?snippet= parameter
     */
    loadCodeFromUrl() {
        const query = new URLSearchParams(window.location.search)

        if (query.has('snippet')) {
            const snippetId = query.get('snippet')
            this.#log('Loading snippet from R2:', snippetId)
            this.loadSnippetFromR2(snippetId)
        }
    }

    /**
     * Load snippet directly from R2 by ID
     * No API call needed - fetch directly from public R2 bucket
     */
    async loadSnippetFromR2(snippetId) {
        try {
            const snippetsBaseUrl = this.snippetsUrlValue
            const baseUrl = `${snippetsBaseUrl}/snippets/${snippetId}`

            const metadataUrl = `${baseUrl}/snippet.json`
            this.#log('Fetching metadata from:', metadataUrl)

            const metadataResponse = await fetch(metadataUrl)
            if (!metadataResponse.ok) {
                throw new Error(`Snippet not found (HTTP ${metadataResponse.status})`)
            }

            const metadata = await metadataResponse.json()

            const expiresAt = new Date(metadata.expires_at)
            if (Date.now() > expiresAt.getTime()) {
                throw new Error('Snippet has expired')
            }

            const codeUrl = `${baseUrl}/code.php`
            const codeResponse = await fetch(codeUrl)
            if (!codeResponse.ok) {
                throw new Error('Code file not found')
            }
            const code = await codeResponse.text()

            this.codeEditorOutlet.setValue(code)

            const datasetFiles = metadata.files
                .filter(f => f.type === 'dataset')

            if (datasetFiles.length > 0) {
                this.#log('Loading', datasetFiles.length, 'dataset(s)')

                if (this.hasWasmOutlet) {
                    if (!this.wasmOutlet.areResourcesLoaded()) {
                        this.#log('WASM resources not loaded yet, waiting...')
                        let attempts = 0
                        while (!this.wasmOutlet.areResourcesLoaded() && attempts < 100) {
                            await new Promise(resolve => setTimeout(resolve, 100))
                            attempts++
                        }

                        if (!this.wasmOutlet.areResourcesLoaded()) {
                            this.#showNotification('WASM resources failed to load, datasets not loaded', 'error')
                            return
                        }
                        this.#log('WASM resources loaded, proceeding with dataset upload')
                    }

                    let loaded = 0
                    let failed = 0

                    for (const datasetFile of datasetFiles) {
                        try {
                            // Fetch dataset from R2
                            const datasetUrl = `${baseUrl}/datasets/${datasetFile.name}`
                            this.#log('Fetching dataset:', datasetUrl)

                            const response = await fetch(datasetUrl)
                            if (!response.ok) {
                                throw new Error(`HTTP ${response.status}`)
                            }

                            const arrayBuffer = await response.arrayBuffer()
                            const uint8Array = new Uint8Array(arrayBuffer)

                            // Upload to WASM filesystem
                            this.#log('Uploading to WASM:', datasetFile.name, `(${uint8Array.length} bytes)`)
                            const success = this.wasmOutlet.uploadFile(datasetFile.name, uint8Array)

                            if (success) {
                                this.#log('Successfully loaded dataset:', datasetFile.name)
                                loaded++
                            } else {
                                this.#logError('uploadFile returned false for:', datasetFile.name)
                                throw new Error('Failed to upload to WASM - uploadFile returned false')
                            }
                        } catch (error) {
                            this.#logError('Failed to load dataset:', datasetFile.name, error)
                            failed++
                        }
                    }

                    this.#log(`Datasets loaded: ${loaded} successful, ${failed} failed`)

                    if (failed > 0) {
                        this.#showNotification(
                            `Loaded ${loaded} dataset(s), ${failed} failed to load`,
                            'warning'
                        )
                    }
                }
            }

            if (this.hasWasmOutlet) {
                this.dispatch('datasets-loaded', { bubbles: true })
            }

            this.dispatch('loaded-from-url', { bubbles: true })
            this.#showNotification('Snippet loaded successfully!', 'success')

            this.#snippetLoaded = true

        } catch (error) {
            console.error('[ShareCode] Failed to load snippet:', error)
            this.#showNotification(`Failed to load snippet: ${error.message}`, 'error')
        }
    }

    /**
     * Share code via API
     */
    async share() {
        if (!this.hasCodeEditorOutlet) {
            this.#logError('Code editor outlet not found')
            return
        }

        const code = this.codeEditorOutlet.getCode()

        try {
            await this.uploadSnippetToAPI(code)
            this.#log()

        } catch (error) {
            this.#logError('[ShareCode] API upload failed:', error)
            this.#showNotification(`Failed to share: ${error.message}`, 'error')
        }
    }

    /**
     * Upload snippet to API
     */
    async uploadSnippetToAPI(code) {
        this.#log('Uploading to API')

        try {
            const turnstileToken = await this.getTurnstileToken()
            const formData = new FormData()
            const codeBlob = new Blob([code], { type: 'text/plain' })

            formData.append('code', codeBlob, 'code.php')

            if (this.hasPlaygroundOutlet && this.hasWasmOutlet) {
                if (typeof this.playgroundOutlet.getSelectedDatasets === 'function') {
                    const datasets = this.playgroundOutlet.getSelectedDatasets()
                    this.#log('Adding', datasets.length, 'datasets')

                    for (let i = 0; i < datasets.length && i < 3; i++) {
                        const dataset = datasets[i]
                        // Read file from WASM
                        const content = this.wasmOutlet.readFile(dataset.path)
                        if (content) {
                            const blob = new Blob([content], { type: 'application/octet-stream' })
                            formData.append(`dataset_${i + 1}`, blob, dataset.name)
                        }
                    }
                }
            }

            const apiUrl = this.apiUrlValue
            const response = await fetch(apiUrl, {
                method: 'POST',
                headers: {
                    'CF-Turnstile-Response': turnstileToken
                },
                body: formData
            })

            const data = await response.json()

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`)
            }

            const snippetId = data.snippet_id
            const shareUrl = `${window.location.origin}${window.location.pathname}?snippet=${snippetId}`

            window.history.pushState({}, '', shareUrl)

            this.#snippetLoaded = true

            if (navigator.clipboard && navigator.clipboard.writeText) {
                try {
                    await navigator.clipboard.writeText(shareUrl)
                    this.#showNotification('Share link copied to clipboard!', 'success', shareUrl)
                } catch (clipboardError) {
                    this.#log('Clipboard write failed (document not focused):', clipboardError)
                    this.#showNotification(`Share link created: ${shareUrl}`, 'success', shareUrl)
                }
            } else {
                prompt('Copy this link:', shareUrl)
            }

            return { success: true }

        } catch (error) {
            console.error('[ShareCode] Upload error:', error)
            throw error
        }
    }

    /**
     * Get Turnstile token (Task 12 - completed)
     */
    async getTurnstileToken() {
        if (!this.hasTurnstileOutlet) {
            this.#logError('Turnstile outlet not available')
            throw new Error('Turnstile not available')
        }

        try {
            const token = await this.turnstileOutlet.getToken()
            this.#log('Obtained Turnstile token')
            return token
        } catch (error) {
            this.#logError('Failed to get Turnstile token:', error)
            throw error
        }
    }

    /**
     * Handle code changes - clear snippet URL when user edits code
     */
    #handleCodeChanged() {
        if (this.#snippetLoaded) {
            this.#log('Code changed, clearing snippet URL from browser')
            const cleanUrl = `${window.location.origin}${window.location.pathname}`
            window.history.pushState({}, '', cleanUrl)
            this.#snippetLoaded = false
        }
    }

    #waitForEditorAndSetValue(code, attempts = 0) {
        const maxAttempts = 50
        if (this.codeEditorOutlet.isReady()) {
            this.codeEditorOutlet.setValue(code)
        } else if (attempts < maxAttempts) {
            setTimeout(() => {
                this.#waitForEditorAndSetValue(code, attempts + 1)
            }, 100)
        }
    }

    #showNotification(message, type = 'info', link = null) {
        this.dispatch('notification', {
            detail: { message, type, link },
            bubbles: true
        })
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[ShareCode]', ...args)
        }
    }

    #logError(...args) {
        console.error('[ShareCode]', ...args)
    }
}
