import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static values = {
        phpJs: String,
        phpWasm: String
    }

    #phpModule = null
    #phpModuleLoaded = false
    #combinedOutput = ''
    #debug = false

    connect() {
        this.#debug = this.application.debug
        this.#log('Connecting WASM controller')
        this.#loadPHPModule()
    }

    disconnect() {
        this.#phpModule = null
        this.#phpModuleLoaded = false
    }

    // Public API for execution
    evaluate(code) {
        if (!this.#phpModuleLoaded) {
            this.#dispatchError('PHP module not loaded yet', null)
            return false
        }

        // Clear output and execute
        // Error reporting is configured in C code (pib_eval.c)
        // Code is passed as-is, including <?php tag
        this.#combinedOutput = ''

        try {
            const result = this.#phpModule.ccall(
                'pib_eval',
                'number',
                ['string'],
                [code]
            )

            // Check if output contains PHP errors (even if execution didn't throw)
            const errorInfo = this.#parseErrorMessage(this.#combinedOutput)
            if (errorInfo) {
                // Output contains an error, dispatch as error
                this.#log('Parsed error info:', errorInfo)
                this.#dispatchError(this.#combinedOutput, errorInfo)
                return false
            }

            const output = this.#combinedOutput || 'Code executed successfully (no output)'
            this.#dispatchOutput(output)
            return true
        } catch (e) {
            this.#logError('Execution error:', e)
            const errorInfo = this.#parseErrorMessage(this.#combinedOutput)
            this.#log('Parsed error info:', errorInfo)
            this.#dispatchError('Execution error: ' + e.message + '\n' + this.#combinedOutput, errorInfo)
            return false
        }
    }

    // Public API for checking ready state
    isReady() {
        return this.#phpModuleLoaded
    }

    // Public API for accessing PHP module (for filesystem operations)
    getModule() {
        return this.#phpModule
    }

    #loadPHPModule() {
        this.#log('Loading PHP WebAssembly module...')
        this.#dispatchProgress('Loading PHP WebAssembly...', 0)

        // Remove old script if exists
        const oldScript = document.getElementById('php-wasm-script')
        if (oldScript) {
            oldScript.remove()
        }

        // Reset state
        this.#phpModuleLoaded = false
        this.#phpModule = null

        // Load the PHP WASM script
        const script = document.createElement('script')
        script.id = 'php-wasm-script'
        script.src = this.phpJsValue

        script.onload = () => {
            this.#log('PHP script loaded, initializing module...')
            this.#dispatchProgress('Initializing PHP runtime...', 30)

            if (typeof PHP !== 'undefined') {
                PHP({
                    print: (text) => {
                        this.#log('PHP stdout:', text)
                        this.#combinedOutput += text + '\n'
                    },
                    printErr: (text) => {
                        this.#log('PHP stderr:', text)
                        this.#combinedOutput += text + '\n'
                    },
                    locateFile: (path, scriptDirectory) => {
                        if (path === 'php.wasm') {
                            this.#log('Locating php.wasm at:', this.phpWasmValue)
                            return this.phpWasmValue
                        }
                        return scriptDirectory + path
                    }
                }).then((module) => {
                    this.#phpModule = module
                    this.#phpModuleLoaded = true
                    this.#log('PHP module initialized successfully')
                    this.#dispatchProgress('Ready!', 100)

                    setTimeout(() => {
                        this.#dispatchReady()
                    }, 500)
                }).catch((err) => {
                    this.#logError('Failed to initialize PHP module:', err)
                    this.#dispatchError('Failed to initialize PHP: ' + err.message)
                })
            } else {
                this.#logError('PHP function not available after script load')
                this.#dispatchError('Failed to load PHP module')
            }
        }

        script.onerror = (err) => {
            this.#logError('Failed to load PHP script:', err)
            this.#dispatchError('Failed to load PHP WebAssembly module')
        }

        document.head.appendChild(script)
    }

    #dispatchReady() {
        this.dispatch('ready')
    }

    #dispatchProgress(message, percent) {
        this.dispatch('progress', { detail: { message, percent } })
    }

    #dispatchOutput(output) {
        this.dispatch('output', { detail: { output } })
    }

    #dispatchError(error, errorInfo) {
        this.dispatch('error', { detail: { error, errorInfo } })
    }

    #parseErrorMessage(output) {
        if (!output) {
            return null
        }

        // Parse error messages from PHP WASM output
        // Example formats:
        // - "At PIB:17" (ParseError, simple)
        // - "called in PIB on line 18" (TypeError, more complex)
        // - "stderr: At PIB:24" (with stderr prefix)

        const patterns = [
            /At PIB:(\d+)/,                    // Matches "At PIB:17"
            /in PIB on line (\d+)/,            // Matches "called in PIB on line 18"
            /stderr: At PIB:(\d+)/             // Matches "stderr: At PIB:24"
        ]

        for (const pattern of patterns) {
            const match = output.match(pattern)
            if (match) {
                const lineNumber = parseInt(match[1], 10)
                // Extract error message - get the first line that contains "throwable" or "Error"
                const errorMessageMatch = output.match(/Uncaught throwable '(\w+)': (.+?)(?:\n|$)/)
                const errorType = errorMessageMatch ? errorMessageMatch[1] : 'Error'
                const errorMessage = errorMessageMatch ? errorMessageMatch[2] : 'Syntax error'

                return {
                    line: lineNumber,
                    type: errorType,
                    message: errorMessage
                }
            }
        }

        return null
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[WASM]', ...args)
        }
    }

    #logError(...args) {
        if (this.#debug) {
            console.error('[WASM]', ...args)
        }
    }
}
