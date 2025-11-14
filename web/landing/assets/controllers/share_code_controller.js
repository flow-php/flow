import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["code-mirror-editor", "playground-editor", "wasm", "turnstile"]
    static values = {
        apiUrl: String,
        snippetsUrl: String
    }
    #debug = false

    connect() {
        this.#debug = this.application.debug
        this.#log('API URL:', this.apiUrlValue)
        this.#log('Snippets URL:', this.snippetsUrlValue)
    }

    codeMirrorEditorOutletConnected() {
        this.loadCodeFromUrl()
    }

    /**
     * Load code from URL - check for ?snippet= parameter
     */
    loadCodeFromUrl() {
        const query = new URLSearchParams(window.location.search)

        // Check for snippet ID
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
            // Construct R2 URL (use configured URL or fallback to production)
            const snippetsBaseUrl = this.snippetsUrlValue || 'https://playground-snippets.flow-php.com'
            const baseUrl = `${snippetsBaseUrl}/snippets/${snippetId}`

            // Fetch metadata first
            const metadataUrl = `${baseUrl}/snippet.json`
            this.#log('Fetching metadata from:', metadataUrl)

            const metadataResponse = await fetch(metadataUrl)
            if (!metadataResponse.ok) {
                throw new Error(`Snippet not found (HTTP ${metadataResponse.status})`)
            }

            const metadata = await metadataResponse.json()

            // Check expiration (90 days - matches backend setting)
            const expiresAt = new Date(metadata.expires_at)
            if (Date.now() > expiresAt.getTime()) {
                throw new Error('Snippet has expired')
            }

            // Fetch code
            const codeUrl = `${baseUrl}/code.php`
            const codeResponse = await fetch(codeUrl)
            if (!codeResponse.ok) {
                throw new Error('Code file not found')
            }
            const code = await codeResponse.text()

            // Load code into editor
            if (this.hasCodeMirrorEditorOutlet && this.codeMirrorEditorOutlet.isReady()) {
                this.codeMirrorEditorOutlet.setValue(code)
            }

            // Load datasets into WASM
            // Extract dataset files from metadata
            const datasetFiles = metadata.files
                .filter(f => f.type === 'dataset')

            if (datasetFiles.length > 0) {
                this.#log('Loading', datasetFiles.length, 'dataset(s)')

                if (this.hasWasmOutlet) {
                    // Wait for WASM resources to be loaded (which creates /workspace/uploads)
                    if (!this.wasmOutlet.areResourcesLoaded()) {
                        this.#log('WASM resources not loaded yet, waiting...')
                        // Wait for resources to load (check every 100ms, max 10 seconds)
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

            // Refresh file browser after loading datasets
            if (this.hasWasmOutlet) {
                this.dispatch('datasets-loaded', { bubbles: true })
            }

            this.dispatch('loaded-from-url', { bubbles: true })
            this.#showNotification('Snippet loaded successfully!', 'success')

        } catch (error) {
            console.error('[ShareCode] Failed to load snippet:', error)
            this.#showNotification(`Failed to load snippet: ${error.message}`, 'error')
        }
    }

    /**
     * Share code via API
     */
    async share() {
        if (!this.hasCodeMirrorEditorOutlet) {
            this.#logError('Code editor outlet not found')
            return
        }

        const code = this.codeMirrorEditorOutlet.getCode()

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
            // Get Turnstile token (placeholder - Task 11 implements)
            const turnstileToken = await this.getTurnstileToken()

            // Prepare FormData
            const formData = new FormData()

            // Add code as Blob (Decision 3)
            const codeBlob = new Blob([code], { type: 'text/plain' })
            formData.append('code', codeBlob, 'code.php')

            // Add datasets from WASM (Task 11 - completed)
            if (this.hasPlaygroundEditorOutlet && this.hasWasmOutlet) {
                // getSelectedDatasets() implemented in Task 11
                if (typeof this.playgroundEditorOutlet.getSelectedDatasets === 'function') {
                    const datasets = this.playgroundEditorOutlet.getSelectedDatasets()
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

            // Upload (use configured API URL or fallback to production)
            const apiUrl = this.apiUrlValue || '/api/playground/snippets'
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

            // Success - construct share URL from snippet ID
            const snippetId = data.snippet_id
            const shareUrl = `${window.location.origin}${window.location.pathname}?snippet=${snippetId}`

            window.history.pushState({}, '', shareUrl)

            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(shareUrl)
                this.#showNotification('Share link copied to clipboard!', 'success', shareUrl)
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

    #waitForEditorAndSetValue(code, attempts = 0) {
        const maxAttempts = 50
        if (this.codeMirrorEditorOutlet.isReady()) {
            this.codeMirrorEditorOutlet.setValue(code)
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
