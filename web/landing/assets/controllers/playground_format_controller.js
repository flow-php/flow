import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["code-editor", "wasm", "playground-output"]

    async format() {
        await Promise.all([
            this.codeEditorOutlet.onLoad(),
            this.wasmOutlet.onLoad()
        ])

        const code = this.codeEditorOutlet.getCode()

        await this.wasmOutlet.writeFile('/workspace/code.php', code)

        this.playgroundOutputOutlet.show({ content: 'Formatting code...', type: 'info' })
        this.element.querySelectorAll('button').forEach(btn => btn.disabled = true)

        const result = await this.wasmOutlet.format(code)

        this.element.querySelectorAll('button').forEach(btn => btn.disabled = false)

        if (result.success) {
            this.codeEditorOutlet.setCode(result.code)
            this.playgroundOutputOutlet.show({ content: 'Code formatted successfully!', type: 'success' })

            if (result.fixers && result.fixers.length > 0) {
                this.playgroundOutputOutlet.append({
                    content: `Applied fixers: ${result.fixers.join(', ')}`,
                    type: 'info'
                })
            }
        } else {
            this.playgroundOutputOutlet.show({ content: `Format error: ${result.error}`, type: 'error' })
        }

        this.dispatch('formatted', { detail: { success: result.success }, bubbles: true })
    }
}
