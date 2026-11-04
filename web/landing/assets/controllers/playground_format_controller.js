import { Controller } from "@hotwired/stimulus"
import { mago } from "../services/mago.js"

export default class extends Controller {
    static outlets = ["code-editor", "wasm", "playground-output"]
    static values = { magoWasm: String, phpVersion: { type: String, default: "8.5" } }

    async format() {
        this.dispatch('action-started', { bubbles: true })
        let success = false

        try {
            await Promise.all([
                this.codeEditorOutlet.onLoad(),
                this.wasmOutlet.onLoad()
            ])

            this.playgroundOutputOutlet.show({ content: 'Formatting code...', type: 'info' })

            const formatted = (await mago(this.magoWasmValue)).format(this.codeEditorOutlet.getCode(), this.phpVersionValue)

            this.codeEditorOutlet.setCode(formatted)
            await this.wasmOutlet.writeFile('/workspace/code.php', formatted)
            this.playgroundOutputOutlet.show({ content: 'Code formatted successfully!', type: 'success' })
            success = true
        } catch (error) {
            this.playgroundOutputOutlet.show({ content: `Format error: ${error}`, type: 'error' })
        } finally {
            this.dispatch('formatted', { detail: { success }, bubbles: true })
            this.dispatch('action-finished', { bubbles: true })
        }
    }
}
