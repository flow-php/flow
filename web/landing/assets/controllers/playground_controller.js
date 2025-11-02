import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["output", "runButton", "loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer"]
    static values = {
        phpJs: String,
        phpWasm: String,
        flowPhar: String
    }

    #phpModule = null
    #phpModuleLoaded = false
    #combinedOutput = ''
    #editor = null
    #debug = false

    connect() {
        this.#debug = this.application.debug
        this.#log('Connecting playground controller')
        // Content is hidden by CSS initially, no need to hide again
        this.#initializeLoadingIndicator()
        this.#loadPHPModule()
    }

    #hideContentDuringLoading() {
        // Hide navigation, editor, and output during loading
        if (this.hasNavigationTarget) {
            this.navigationTarget.style.display = 'none'
        }
        if (this.hasEditorTarget) {
            this.editorTarget.style.display = 'none'
        }
        if (this.hasOutputContainerTarget) {
            this.outputContainerTarget.style.display = 'none'
        }
    }

    #showContentAfterLoading() {
        // Show navigation, editor, and output after loading
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

    disconnect() {
        this.#phpModule = null
        this.#phpModuleLoaded = false
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[Playground]', ...args)
        }
    }

    #logError(...args) {
        if (this.#debug) {
            console.error('[Playground]', ...args)
        }
    }

    #initializeLoadingIndicator() {
        if (this.hasLoadingMessageTarget) {
            this.#showLoading('Loading PHP WebAssembly...', 0)
        }
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
        // Show the content after loading is complete
        this.#showContentAfterLoading()
    }

    #loadPHPModule() {
        this.#log('Loading PHP WebAssembly module...')
        this.#showLoading('Loading PHP WebAssembly...', 0)

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
            this.#showLoading('Initializing PHP runtime...', 30)

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
                        // For php.wasm, use the path we got from Stimulus values
                        if (path === 'php.wasm') {
                            this.#log('Locating php.wasm at:', this.phpWasmValue)
                            return this.phpWasmValue
                        }
                        // For other files, use default behavior
                        return scriptDirectory + path
                    }
                }).then((module) => {
                    this.#phpModule = module
                    this.#phpModuleLoaded = true
                    this.#log('PHP module initialized successfully')
                    this.#showLoading('Loading Flow PHP library...', 60)

                    // Load Flow phar
                    this.#loadFlowPhar(() => {
                        this.#log('Flow phar loaded successfully')
                        this.#showLoading('Ready!', 100)
                        setTimeout(() => {
                            this.#hideLoading()
                            this.#showFlowVersion()
                        }, 500)
                    })
                }).catch((err) => {
                    this.#logError('Failed to initialize PHP module:', err)
                    this.#hideLoading()
                    this.#showError('Failed to initialize PHP: ' + err.message)
                })
            } else {
                this.#logError('PHP function not available after script load')
                this.#hideLoading()
                this.#showError('Failed to load PHP module')
            }
        }

        script.onerror = (err) => {
            this.#logError('Failed to load PHP script:', err)
            this.#hideLoading()
            this.#showError('Failed to load PHP WebAssembly module')
        }

        document.head.appendChild(script)
    }

    #loadFlowPhar(callback) {
        // Extract filename from path for the WASM filesystem
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
                this.#showError('Failed to load Flow: ' + err.message)
            })
    }

    #showError(message) {
        // Show content and display error in output
        this.#showContentAfterLoading()
        if (this.hasOutputTarget) {
            this.outputTarget.textContent = message
        }
    }

    #showFlowVersion() {
        if (this.hasOutputTarget) {
            this.outputTarget.textContent = 'Click "Run" to execute your code.'
        }
    }

    run(event) {
        event.preventDefault()

        if (!this.#phpModuleLoaded) {
            if (this.hasOutputTarget) {
                this.outputTarget.textContent = 'PHP module not loaded yet, please wait...'
            }
            return
        }

        // Get the code editor value
        const editorTextarea = document.getElementById('features_example')
        if (!editorTextarea) {
            this.#logError('Editor textarea not found')
            return
        }

        let userCode = editorTextarea.value

        // Strip PHP opening tag if present (eval doesn't need it)
        userCode = userCode.replace(/^<\?php\s*/i, '')

        // Create the runner code - use filename from the path
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

        this.#combinedOutput = ''
        if (this.hasOutputTarget) {
            this.outputTarget.textContent = 'Running...'
        }

        // Execute the code
        try {
            const result = this.#phpModule.ccall(
                'pib_eval',
                'number',
                ['string'],
                [runnerCode]
            )

            if (this.hasOutputTarget) {
                if (this.#combinedOutput) {
                    this.outputTarget.textContent = this.#combinedOutput
                } else {
                    this.outputTarget.textContent = 'Code executed successfully (no output)'
                }
            }
        } catch (e) {
            this.#logError('Execution error:', e)
            if (this.hasOutputTarget) {
                this.outputTarget.textContent = 'Execution error: ' + e.message + '\n' + this.#combinedOutput
            }
        }
    }

    setEditor(editor) {
        this.#editor = editor
    }
}
