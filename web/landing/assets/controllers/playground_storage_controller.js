import { Controller } from '@hotwired/stimulus'

/**
 * I had to temporarily disable the code saving functionality. It was causing issues with sharring code snippets
 * and creating snippets fingerprint. I will re-enable it in the future.
 */
export default class extends Controller {
    static outlets = ['code-editor', 'wasm']
    static values = {
        debounceMs: { type: Number, default: 1000 },
        storageKey: { type: String, default: 'flow-playground-code' }
    }

    #debounceTimer = null
    // #boundHandleCodeChanged = null
    #wasmResourcesLoaded = false
    #pendingCodeLoad = false

    connect() {
        // this.#boundHandleCodeChanged = this.#handleCodeChanged.bind(this)
        // this.element.addEventListener('code-editor:code-changed', this.#boundHandleCodeChanged)
    }

    disconnect() {
        // if (this.#boundHandleCodeChanged) {
        //     this.element.removeEventListener('code-editor:code-changed', this.#boundHandleCodeChanged)
        // }
        if (this.#debounceTimer) {
            clearTimeout(this.#debounceTimer)
        }
    }

    onWasmResourcesLoaded() {
        this.#wasmResourcesLoaded = true

        if (this.#pendingCodeLoad) {
            this.loadCode().catch(error => {
                console.error('Failed to load code from storage:', error)
            })
        }
    }

    codeEditorOutletConnected() {
        const hasSharedSnippet = new URLSearchParams(window.location.search).has('snippet')
        if (!hasSharedSnippet) {
            if (this.#wasmResourcesLoaded) {
                this.loadCode()
            } else {
                this.#pendingCodeLoad = true
            }
        }
    }

    async loadCode() {
        try {
            if (!this.hasCodeEditorOutlet) {
                console.warn('Code editor outlet not available')
                return
            }
                if (!this.hasWasmOutlet) {
                    console.warn('WASM outlet not available')
                    return
                }

                await Promise.all([
                    this.codeEditorOutlet.onLoad(),
                    this.wasmOutlet.onLoad()
                ])

                const result = await this.wasmOutlet.readFile('/workspace/code.php')
                if (result.success && result.content) {
                    this.codeEditorOutlet.setCode(result.content)
                    this.dispatch('loaded', { detail: { codeLength: result.content.length }, bubbles: true })
                } else {
                    console.warn('Failed to read /workspace/code.php:', result)
                }
        } catch (error) {
            console.error('Error in loadCode:', error)
        }
    }

    clearCode() {
        localStorage.removeItem(this.storageKeyValue)
        this.dispatch('cleared', { detail: { timestamp: Date.now() }, bubbles: true })
    }

    #handleCodeChanged() {
        if (this.#debounceTimer) clearTimeout(this.#debounceTimer)
        this.#debounceTimer = setTimeout(() => this.#save(), this.debounceMsValue)
    }

    #save() {
        // const code = this.codeEditorOutlet.getCode()
        // localStorage.setItem(this.storageKeyValue, code)
        // this.dispatch('saved', { detail: { codeLength: code.length }, bubbles: true })
    }
}
