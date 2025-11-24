import { Controller } from "@hotwired/stimulus"
import { createFingerprint } from "../services/snippet-fingerprint.js"

export default class extends Controller {
    static outlets = ["code-editor", "wasm", "turnstile", "playground-upload"]
    static values = {
        apiUrl: String,
        snippetsUrl: String
    }
    #debug = false
    #snippetLoaded = false
    #boundHandleCodeChanged = null
    #wasmResourcesLoaded = false
    #pendingSnippetLoad = false
    #defaultFingerprint = null

    connect() {
        this.#debug = this.application.debug
        this.#log('API URL:', this.apiUrlValue)
        this.#log('Snippets URL:', this.snippetsUrlValue)

        this.#boundHandleCodeChanged = this.#handleCodeChanged.bind(this)
        this.element.addEventListener('code-editor:code-changed', this.#boundHandleCodeChanged)
    }

    disconnect() {
        if (this.#boundHandleCodeChanged) {
            this.element.removeEventListener('code-editor:code-changed', this.#boundHandleCodeChanged)
        }
    }

    async onWasmResourcesLoaded() {
        this.#log('WASM resources loaded in share controller')
        this.#wasmResourcesLoaded = true

        this.#createDefaultFingerprint()

        if (this.#pendingSnippetLoad) {
            this.#log('Loading pending snippet')
            this.loadCodeFromUrl()
        }
    }

    async #createDefaultFingerprint() {
        try {
            const result = await this.wasmOutlet.readFile('/workspace/code.php')
            if (result.success) {
                let defaultCode
                if (typeof result.content === 'string') {
                    defaultCode = result.content
                } else if (result.content instanceof Uint8Array) {
                    defaultCode = new TextDecoder().decode(result.content)
                } else if (result.content && result.content.buffer) {
                    defaultCode = new TextDecoder().decode(result.content.buffer)
                } else {
                    this.#logError('Unexpected content type from readFile:', typeof result.content)
                    return
                }
                this.#defaultFingerprint = await createFingerprint(defaultCode, [])
                this.#log('Default code fingerprint:', this.#defaultFingerprint)
            }
        } catch (error) {
            this.#logError('Failed to create default fingerprint:', error)
        }
    }

    async #checkSnippetExistsInR2(snippetId) {
        try {
            const snippetsBaseUrl = this.snippetsUrlValue
            const metadataUrl = `${snippetsBaseUrl}/snippets/${snippetId}/snippet.json`

            this.#log('Checking if snippet exists in R2:', metadataUrl)

            const response = await fetch(metadataUrl, { method: 'HEAD' })

            if (response.ok) {
                this.#log('Snippet exists in R2')
                return true
            }

            this.#log('Snippet does not exist in R2')
            return false
        } catch (error) {
            this.#logError('Failed to check R2:', error)
            return false
        }
    }

    wasmOutletConnected() {
        const query = new URLSearchParams(window.location.search)
        const hasSnippet = query.has('snippet')

        if (!hasSnippet) {
            return
        }

        if (this.#wasmResourcesLoaded) {
            this.loadCodeFromUrl()
        } else {
            this.#log('WASM not ready yet, deferring snippet load')
            this.#pendingSnippetLoad = true
        }
    }

    loadCodeFromUrl() {
        const query = new URLSearchParams(window.location.search)

        if (query.has('snippet')) {
            const snippetId = query.get('snippet')
            this.#log('Loading snippet from R2:', snippetId)
            this.#pendingSnippetLoad = false
            this.#loadSnippetFromR2(snippetId)
        }
    }

    async #loadSnippetFromR2(snippetId) {
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

            this.codeEditorOutlet.setCode(code)

            await this.wasmOutlet.writeFile('/workspace/code.php', code)

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
                            const datasetUrl = `${baseUrl}/datasets/${datasetFile.name}`
                            this.#log('Fetching dataset:', datasetUrl)

                            const response = await fetch(datasetUrl)
                            if (!response.ok) {
                                throw new Error(`HTTP ${response.status}`)
                            }

                            const arrayBuffer = await response.arrayBuffer()
                            const uint8Array = new Uint8Array(arrayBuffer)

                            this.#log('Uploading to WASM:', datasetFile.name, `(${uint8Array.length} bytes)`)
                            const result = await this.wasmOutlet.writeFile(
                                `/workspace/uploads/${datasetFile.name}`,
                                uint8Array
                            )

                            if (result.success) {
                                this.#log('Successfully loaded dataset:', datasetFile.name)
                                loaded++
                            } else {
                                this.#logError('writeFile returned false for:', datasetFile.name)
                                throw new Error('Failed to upload to WASM - writeFile returned false')
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

            this.dispatch('datasets-loaded', { bubbles: true })
            this.dispatch('loaded-from-url', { bubbles: true })
            this.#showNotification('Snippet loaded successfully!', 'success')

            this.#snippetLoaded = true

        } catch (error) {
            console.error('[ShareCode] Failed to load snippet:', error)
            this.#showNotification(`Failed to load snippet: ${error.message}`, 'error')
        }
    }

    async share() {
        await Promise.all([
            this.codeEditorOutlet.onLoad(),
            this.wasmOutlet.onLoad(),
            this.turnstileOutlet.onLoad()
        ])

        const code = this.codeEditorOutlet.getCode()

        await this.wasmOutlet.writeFile('/workspace/code.php', code)

        try {
            const files = await this.#collectUploadedFiles()
            const fileBlobs = files.map(f => f.blob)
            const fingerprint = await createFingerprint(code, fileBlobs)

            this.#log('Snippet fingerprint:', fingerprint)

            if (this.#defaultFingerprint && fingerprint === this.#defaultFingerprint) {
                this.#log('Code matches default, not uploading')
                this.#showNotification('You are trying to share default code. Please modify the code first.', 'info')
                return
            }

            const snippetExists = await this.#checkSnippetExistsInR2(fingerprint)
            if (snippetExists) {
                this.#log('Snippet already exists in R2, skipping upload')
                const shareUrl = `${window.location.origin}${window.location.pathname}?snippet=${fingerprint}`

                const currentUrl = new URL(window.location.href)
                const currentSnippetId = currentUrl.searchParams.get('snippet')

                if (currentSnippetId !== fingerprint) {
                    window.history.pushState({}, '', shareUrl)
                    this.#snippetLoaded = true
                }

                if (navigator.clipboard && navigator.clipboard.writeText) {
                    try {
                        await navigator.clipboard.writeText(shareUrl)
                        this.#showNotification('Snippet already exists! Link copied to clipboard.', 'success', shareUrl)
                    } catch (clipboardError) {
                        this.#log('Clipboard write failed:', clipboardError)
                        this.#showNotification(`Snippet already exists: ${shareUrl}`, 'success', shareUrl)
                    }
                } else {
                    prompt('Copy this link:', shareUrl)
                }
                return
            }

            await this.#uploadSnippetToAPI(code, fingerprint, files)
        } catch (error) {
            console.error('[ShareCode] Share failed:', error)
            console.error('[ShareCode] Error stack:', error.stack)
            this.#logError('[ShareCode] Share failed:', error)
            this.#showNotification(`Failed to share: ${error.message}`, 'error')
        }
    }

    async #uploadSnippetToAPI(code, snippetId, files) {
        this.#log('Uploading to API with snippet ID:', snippetId)

        try {
            const turnstileToken = await this.#getTurnstileToken()
            const formData = new FormData()
            const codeBlob = new Blob([code], { type: 'text/plain' })

            formData.append('snippet_id', snippetId)
            formData.append('code', codeBlob, 'code.php')

            this.#log('Adding', files.length, 'datasets')
            for (let i = 0; i < files.length && i < 3; i++) {
                const file = files[i]
                formData.append(`dataset_${i + 1}`, file.blob, file.name)
            }

            const apiUrl = this.apiUrlValue
            this.#log('Sending request to:', apiUrl)

            const response = await fetch(apiUrl, {
                method: 'POST',
                headers: {
                    'CF-Turnstile-Response': turnstileToken
                },
                body: formData
            })

            this.#log('Response status:', response.status)
            const data = await response.json()
            this.#log('Response data:', data)

            if (!response.ok || !data.success) {
                this.#logError('API error response:', data)
                throw new Error(data.error || `HTTP ${response.status}`)
            }

            const shareUrl = `${window.location.origin}${window.location.pathname}?snippet=${snippetId}`

            const currentUrl = new URL(window.location.href)
            const currentSnippetId = currentUrl.searchParams.get('snippet')

            if (currentSnippetId !== snippetId) {
                window.history.pushState({}, '', shareUrl)
                this.#snippetLoaded = true
            }

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
        } catch (error) {
            console.error('[ShareCode] Upload error:', error)
            throw error
        }
    }

    async #getTurnstileToken() {
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

    #handleCodeChanged() {
        if (this.#snippetLoaded) {
            this.#log('Code changed, clearing snippet URL from browser')
            const cleanUrl = `${window.location.origin}${window.location.pathname}`
            window.history.pushState({}, '', cleanUrl)
            this.#snippetLoaded = false
        }
    }

    async #collectUploadedFiles() {
        const files = []

        if (this.hasPlaygroundUploadOutlet) {
            const datasets = await this.playgroundUploadOutlet.listFiles()

            for (let i = 0; i < datasets.length && i < 3; i++) {
                const dataset = datasets[i]

                if (!dataset.name || dataset.name.trim().length === 0) {
                    this.#logError('Skipping dataset with empty name:', dataset)
                    continue
                }

                const result = await this.wasmOutlet.readFile(dataset.path)
                if (result.success) {
                    const blob = new Blob([result.content], { type: 'application/octet-stream' })
                    files.push({ name: dataset.name, blob })
                }
            }
        }

        return files
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
