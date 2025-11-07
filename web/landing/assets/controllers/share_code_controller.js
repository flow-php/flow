import { Controller } from "@hotwired/stimulus"
import LZString from "lz-string"

export default class extends Controller {
    static outlets = ["code-mirror-editor", "playground-editor"]
    #debug = false

    connect() {
        this.#debug = this.application.debug
    }

    codeMirrorEditorOutletConnected() {
        this.loadCodeFromUrl()
    }

    loadCodeFromUrl() {
        const query = new URLSearchParams(window.location.search)

        if (query.has('c')) {
            try {
                const compressed = query.get('c')
                this.#log('[ShareCode] Compressed code:', compressed)

                const decompressed = LZString.decompressFromEncodedURIComponent(compressed)
                this.#log('[ShareCode] Decompressed code:', decompressed)
                this.#log('[ShareCode] Has code editor outlet:', this.hasCodeMirrorEditorOutlet)

                if (decompressed && this.hasCodeMirrorEditorOutlet) {
                    // Wait for editor to be ready before setting value
                    this.#waitForEditorAndSetValue(decompressed)
                    this.dispatch('loaded-from-url', { bubbles: true })
                } else if (!decompressed) {
                    console.error('[ShareCode] Decompression returned null or empty')
                }
            } catch (e) {
                console.error('Failed to decompress code:', e)
            }
        }
    }

    #waitForEditorAndSetValue(code, attempts = 0) {
        const maxAttempts = 50 // 5 seconds max

        if (this.codeMirrorEditorOutlet.isReady()) {
            this.#log('[ShareCode] Editor is ready, setting value...')
            this.codeMirrorEditorOutlet.setValue(code)
        } else if (attempts < maxAttempts) {
            this.#log('[ShareCode] Editor not ready yet, waiting... (attempt', attempts + 1, ')')
            setTimeout(() => {
                this.#waitForEditorAndSetValue(code, attempts + 1)
            }, 100)
        } else {
            this.#logError('[ShareCode] Editor did not become ready in time')
        }
    }

    share() {
        if (!this.hasCodeMirrorEditorOutlet) {
            this.#logError('Code editor outlet not found')
            return
        }

        const code = this.codeMirrorEditorOutlet.getCode()
        this.#log('[ShareCode] Original code to compress:', code)

        const url = new URL(window.location.href)
        url.search = ''

        const compressed = LZString.compressToEncodedURIComponent(code)
        this.#log('[ShareCode] Compressed code:', compressed)

        url.searchParams.set('c', compressed)

        const link = url.toString()
        this.#log('[ShareCode] Share link:', link)

        window.history.pushState({}, '', link)

        if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(link).then(() => {
                this.#showNotification('Share link copied to clipboard!', 'success', link)
            }).catch((err) => {
                console.error('Failed to copy:', err)
                prompt('Copy this link:', link)
            })
        } else {
            prompt('Copy this link:', link)
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
            console.log('[Code Sharing]', ...args)
        }
    }

    #logError(...args) {
        if (this.#debug) {
            console.error('[Code Sharing]', ...args)
        }
    }
}
