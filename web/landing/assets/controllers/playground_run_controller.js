import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["code-editor", "wasm", "playground-output"]

    async run() {
        await Promise.all([
            this.codeEditorOutlet.onLoad(),
            this.wasmOutlet.onLoad()
        ])

        const code = this.codeEditorOutlet.getCode()

        await this.wasmOutlet.writeFile('/workspace/code.php', code)

        this.codeEditorOutlet.clearErrors()
        this.playgroundOutputOutlet.clear()
        this.playgroundOutputOutlet.show({ content: 'Running...', type: 'info' })

        this.element.querySelectorAll('button').forEach(btn => btn.disabled = true)

        const result = await this.wasmOutlet.run(code)

        this.element.querySelectorAll('button').forEach(btn => btn.disabled = false)

        if (result.success) {
            this.playgroundOutputOutlet.show({ content: "\n" + result.output, type: 'success' })
        } else {
            this.playgroundOutputOutlet.show({ content: result.error.message, type: 'error' })

            if (result.error.line) {
                this.codeEditorOutlet.highlightError({
                    line: result.error.line,
                    type: result.error.type,
                    message: result.error.message,
                    column: result.error.column
                })
            }
        }

        this.dispatch('executed', {
            detail: { success: result.success, executionTime: result.executionTime },
            bubbles: true
        })
    }
}
