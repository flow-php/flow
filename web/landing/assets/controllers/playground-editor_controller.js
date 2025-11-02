import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["runButton", "output", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer"]
    static outlets = ["code-editor"]

    connect() {
        // Content is hidden by CSS initially
        this.#log('Connecting editor controller')
    }

    // Called when WASM is ready via event
    onWasmReady(event) {
        this.#log('WASM ready, hiding loading and showing content')
        this.#hideLoading()
        this.#showOutput('Click "Run" to execute your code.')
    }

    // Called when WASM reports progress
    onWasmProgress(event) {
        const { message, percent } = event.detail
        this.#showLoading(message, percent)
    }

    // Called when WASM has output
    onWasmOutput(event) {
        const { output } = event.detail
        this.#showOutput(output)
    }

    // Called when WASM has an error
    onWasmError(event) {
        const { error } = event.detail
        this.#hideLoading()
        this.#showContentAfterLoading()
        this.#showOutput(error)
    }

    // Run button click handler
    run(event) {
        event.preventDefault()

        // Get WASM controller from same element
        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        // Check if WASM is ready
        if (!wasmController.isReady()) {
            this.#showOutput('PHP module not loaded yet, please wait...')
            return
        }

        // Get code from code-editor outlet
        if (!this.hasCodeEditorOutlet) {
            this.#log('Code editor outlet not connected')
            this.#showOutput('Code editor not found')
            return
        }

        const code = this.codeEditorOutlet.getCode()
        this.#showOutput('Running...')

        // Execute code via WASM controller
        wasmController.evaluate(code)
    }

    #getWasmController() {
        return this.application.getControllerForElementAndIdentifier(
            this.element,
            'wasm'
        )
    }

    #showLoading(message, percent) {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = message
        }
        if (this.hasLoadingPercentTarget && percent !== undefined) {
            this.loadingPercentTarget.textContent = Math.round(percent) + '%'
        }
        if (this.hasLoadingBarTarget && percent !== undefined) {
            this.loadingBarTarget.style.width = percent + '%'
        }
        if (this.hasRunButtonTarget) {
            this.runButtonTarget.disabled = true
        }
    }

    #hideLoading() {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = ''
        }
        if (this.hasLoadingPercentTarget) {
            this.loadingPercentTarget.textContent = ''
        }
        if (this.hasLoadingBarTarget) {
            this.loadingBarTarget.style.width = '0%'
        }
        if (this.hasRunButtonTarget) {
            this.runButtonTarget.disabled = false
        }
        this.#showContentAfterLoading()
    }

    #showContentAfterLoading() {
        if (this.hasNavigationTarget) {
            this.navigationTarget.style.display = 'grid'
        }
        if (this.hasEditorTarget) {
            this.editorTarget.style.display = 'grid'
        }
        if (this.hasOutputContainerTarget) {
            this.outputContainerTarget.style.display = 'block'
        }
    }

    #showOutput(message) {
        if (this.hasOutputTarget) {
            this.outputTarget.textContent = message
        }
    }

    #log(...args) {
        if (this.application.debug) {
            console.log('[Editor]', ...args)
        }
    }
}
