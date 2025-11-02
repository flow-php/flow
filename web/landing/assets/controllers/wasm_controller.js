import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static values = {
        phpJs: String,
        phpWasm: String,
        flowPhar: String
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
            this.#dispatchError('PHP module not loaded yet')
            return false
        }

        // Strip PHP opening tag if present (eval doesn't need it)
        const userCode = code.replace(/^<\?php\s*/i, '')

        // Create the runner code
        const flowPharFilename = this.flowPharValue.split('/').pop()
        const runnerCode = `
error_reporting(E_ALL);
ini_set('display_errors', 'stdout');

// Check if phar file exists
if (!file_exists('/${flowPharFilename}')) {
    echo "Error: Flow phar file not found at: /${flowPharFilename}\\n";
    echo "Current directory: " . getcwd() . "\\n";
    echo "Files in root: " . print_r(scandir('/'), true) . "\\n";
    exit(1);
}

// Load the Flow phar
require_once 'phar:///${flowPharFilename}/vendor/autoload.php';

try {
    // Execute the user's code
    eval(${JSON.stringify(userCode)});
} catch (\\Throwable $e) {
    echo "Error: " . $e->getMessage() . "\\n";
    echo "File: " . $e->getFile() . "\\n";
    echo "Line: " . $e->getLine() . "\\n";
    echo "\\nStack trace:\\n" . $e->getTraceAsString() . "\\n";
}
`

        // Clear output and execute
        this.#combinedOutput = ''

        try {
            const result = this.#phpModule.ccall(
                'pib_eval',
                'number',
                ['string'],
                [runnerCode]
            )

            const output = this.#combinedOutput || 'Code executed successfully (no output)'
            this.#dispatchOutput(output)
            return true
        } catch (e) {
            this.#logError('Execution error:', e)
            this.#dispatchError('Execution error: ' + e.message + '\n' + this.#combinedOutput)
            return false
        }
    }

    // Public API for checking ready state
    isReady() {
        return this.#phpModuleLoaded
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
                    this.#dispatchProgress('Loading Flow PHP library...', 60)

                    // Load Flow phar
                    this.#loadFlowPhar(() => {
                        this.#log('Flow phar loaded successfully')
                        this.#dispatchProgress('Ready!', 100)
                        setTimeout(() => {
                            this.#dispatchReady()
                        }, 500)
                    })
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

    #loadFlowPhar(callback) {
        const flowPharFilename = this.flowPharValue.split('/').pop()
        this.#log('Loading Flow phar:', flowPharFilename, 'from', this.flowPharValue)

        // Check if already loaded
        try {
            const stats = this.#phpModule.FS.stat('/' + flowPharFilename)
            if (stats) {
                this.#log('Flow phar already loaded')
                if (callback) callback()
                return
            }
        } catch (e) {
            // File doesn't exist, need to load it
        }

        // Fetch and load the phar
        fetch(this.flowPharValue)
            .then((response) => {
                if (!response.ok) {
                    throw new Error('Failed to fetch Flow phar: ' + response.status)
                }
                return response.arrayBuffer()
            })
            .then((buffer) => {
                const uint8Array = new Uint8Array(buffer)
                this.#phpModule.FS.writeFile('/' + flowPharFilename, uint8Array)
                if (callback) callback()
            })
            .catch((err) => {
                this.#logError('Error loading Flow phar:', err)
                this.#dispatchError('Failed to load Flow: ' + err.message)
            })
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

    #dispatchError(error) {
        this.dispatch('error', { detail: { error } })
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
