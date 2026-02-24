import { Controller } from '@hotwired/stimulus'

export default class extends Controller {
    static outlets = ['code-editor', 'wasm']
    static values = {
        storageKey: { type: String, default: 'flow-playground-code' },
        initialCode: { type: String, default: '' }
    }

    #wasmResourcesLoaded = false
    #pendingCodeLoad = false

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

            // If initial code is provided (from example), use that
            if (this.hasInitialCodeValue && this.initialCodeValue) {
                this.codeEditorOutlet.setCode(this.initialCodeValue)
                await this.wasmOutlet.writeFile('/workspace/code.php', this.initialCodeValue)
                this.dispatch('loaded-from-example', { detail: { codeLength: this.initialCodeValue.length }, bubbles: true })
                return
            }

            // Otherwise, load from WASM default
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
}
