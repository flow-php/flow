import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["runButton", "output", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer"]
    static outlets = ["code-editor"]
    static values = {
        flowPhar: String
    }

    #flowPharLoaded = false

    connect() {
        // Content is hidden by CSS initially
        this.#log('Connecting editor controller')
    }

    // Called when WASM is ready via event
    onWasmReady(event) {
        this.#log('WASM ready, loading Flow PHAR')
        this.#loadFlowPhar()
    }

    #loadFlowPhar() {
        const wasmController = this.#getWasmController()
        if (!wasmController) {
            this.#log('WASM controller not found')
            return
        }

        this.#showOutput('Loading Flow PHP library...')

        // Fetch and load flow.phar into WASM filesystem
        fetch(this.flowPharValue)
            .then(response => {
                if (!response.ok) {
                    throw new Error('Failed to fetch Flow PHAR: ' + response.status)
                }
                return response.arrayBuffer()
            })
            .then(buffer => {
                const uint8Array = new Uint8Array(buffer)
                // Write to WASM filesystem as 'flow.phar'
                wasmController.getModule().FS.writeFile('/flow.phar', uint8Array)
                this.#flowPharLoaded = true
                this.#log('Flow PHAR loaded successfully')
                this.#hideLoading()
                this.#showOutput('Click "Run" to execute your code.')
            })
            .catch(err => {
                this.#log('Error loading Flow PHAR:', err)
                this.#showOutput('Error: Failed to load Flow PHP library: ' + err.message)
            })
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
        const { error, errorInfo } = event.detail
        this.#log('Error event received:', { error, errorInfo })
        this.#hideLoading()
        this.#showContentAfterLoading()
        this.#showOutput(error)

        // Highlight error in code editor if we have errorInfo
        if (errorInfo && this.hasCodeEditorOutlet) {
            this.#log('Highlighting error:', errorInfo)
            this.codeEditorOutlet.highlightError(errorInfo)
        } else {
            this.#log('No errorInfo or code editor outlet not available', {
                hasErrorInfo: !!errorInfo,
                hasOutlet: this.hasCodeEditorOutlet
            })
        }
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

        // Clear previous errors before running
        this.codeEditorOutlet.clearErrors()

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
