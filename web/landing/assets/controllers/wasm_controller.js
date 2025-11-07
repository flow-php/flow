import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static values = {
        phpJs: String,
        phpWasm: String,
        resources: Object
    }

    #phpModule = null
    #phpModuleLoaded = false
    #resourcesLoaded = false
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
        this.#resourcesLoaded = false
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
            // Change working directory to /workspace before execution
            try {
                this.#phpModule.FS.chdir('/workspace')
                this.#log('Changed working directory to /workspace')
            } catch (chdirError) {
                this.#logError('Failed to change directory to /workspace:', chdirError)
            }

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

    formatCode(code, callback) {
        if (!this.#phpModuleLoaded) {
            callback(null, 'PHP module not loaded yet', null)
            return
        }

        this.#combinedOutput = ''

        try {
            this.#phpModule.FS.chdir('/workspace')

            const tempFile = '/workspace/temp_format.php'

            this.#phpModule.FS.writeFile(tempFile, code)

            const formatScript = `<?php
\$argc = 2;
\$argv = ['cs-fixer.php', '${tempFile}'];
require '/workspace/bin/cs-fixer.php';
?>`

            this.#phpModule.ccall(
                'pib_eval',
                'number',
                ['string'],
                [formatScript]
            )

            const output = this.#combinedOutput.trim()

            if (output.includes('ERROR:')) {
                const errorMsg = output.replace('ERROR:', '').trim()
                callback(null, errorMsg, null)
            } else if (output.includes('SUCCESS')) {
                try {
                    const formattedCode = this.#phpModule.FS.readFile(tempFile, { encoding: 'utf8' })

                    const appliedFixers = this.#parseAppliedFixers(output)

                    callback(formattedCode, null, appliedFixers)
                } catch (readError) {
                    callback(null, 'Failed to read formatted code: ' + readError.message, null)
                }
            } else {
                callback(null, 'Unexpected output from formatter: ' + output, null)
            }

            try {
                this.#phpModule.FS.unlink(tempFile)
            } catch (e) {
                this.#log('Failed to delete temp file:', e)
            }

        } catch (error) {
            this.#logError('Format error:', error)
            callback(null, 'Format failed: ' + error.message, null)
        }
    }

    #parseAppliedFixers(output) {
        const match = output.match(/APPLIED_FIXERS:(.+)/);
        if (match && match[1]) {
            return match[1].split(',').filter(f => f.trim());
        }
        return [];
    }

    // Public API for checking ready state
    isReady() {
        return this.#phpModuleLoaded
    }

    // Public API for accessing PHP module (for filesystem operations)
    getModule() {
        return this.#phpModule
    }

    // Public API for listing files in WASM filesystem
    listFiles(path = '/') {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot list files: PHP module not loaded yet')
            return []
        }

        try {
            const files = []
            const FS = this.#phpModule.FS

            const readDirectory = (dirPath) => {
                try {
                    const entries = FS.readdir(dirPath)

                    for (const entry of entries) {
                        // Skip . and ..
                        if (entry === '.' || entry === '..') {
                            continue
                        }

                        const fullPath = dirPath === '/' ? '/' + entry : dirPath + '/' + entry

                        try {
                            const stat = FS.stat(fullPath)

                            if (FS.isDir(stat.mode)) {
                                files.push({
                                    name: entry,
                                    path: fullPath,
                                    type: 'directory'
                                })
                                // Recursively read subdirectories
                                readDirectory(fullPath)
                            } else {
                                files.push({
                                    name: entry,
                                    path: fullPath,
                                    type: 'file'
                                })
                            }
                        } catch (statError) {
                            this.#logError(`Error stating ${fullPath}:`, statError)
                        }
                    }
                } catch (readdirError) {
                    this.#logError(`Error reading directory ${dirPath}:`, readdirError)
                }
            }

            readDirectory(path)
            return files
        } catch (error) {
            this.#logError('Error listing files:', error)
            return []
        }
    }

    // Public API for reading file content from WASM filesystem
    readFile(filePath) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot read file: PHP module not loaded yet')
            return null
        }

        try {
            const FS = this.#phpModule.FS
            const content = FS.readFile(filePath, { encoding: 'utf8' })
            this.#log('File read successfully:', filePath)
            return content
        } catch (error) {
            this.#logError('Error reading file:', error)
            return null
        }
    }

    // Public API for uploading user files to /workspace/tmp
    uploadFile(filename, uint8Array) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot upload file: PHP module not loaded yet')
            return false
        }

        try {
            const tmpDir = '/workspace/tmp'

            try {
                const pathInfo = this.#phpModule.FS.analyzePath(tmpDir)
                if (!pathInfo.exists) {
                    this.#log(`Creating ${tmpDir} directory`)
                    this.#phpModule.FS.mkdir(tmpDir)
                }
            } catch (dirError) {
                this.#logError(`Error checking/creating ${tmpDir}:`, dirError)
                return false
            }

            const filePath = tmpDir + '/' + filename

            this.#phpModule.FS.writeFile(filePath, uint8Array)
            this.#log(`Successfully uploaded: ${filePath} (${uint8Array.length} bytes)`)

            return true
        } catch (error) {
            this.#logError(`Error uploading file ${filename}:`, error)
            return false
        }
    }

    // Public API for loading resources into WASM filesystem
    async loadResources() {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot load resources: PHP module not loaded yet')
            return false
        }

        if (this.#resourcesLoaded) {
            this.#log('Resources already loaded')
            return true
        }

        if (!this.resourcesValue || Object.keys(this.resourcesValue).length === 0) {
            this.#log('No resources configured to load')
            this.#resourcesLoaded = true
            this.#dispatchResourcesLoaded()
            return true
        }

        this.#log('Loading resources into WASM filesystem...', this.resourcesValue)

        try {
            // Create /workspace directory
            this.#log('Creating /workspace directory')
            this.#phpModule.FS.mkdir('/workspace')

            const resources = Object.entries(this.resourcesValue)
            const totalResources = resources.length
            let loadedCount = 0

            for (const [virtualPath, assetUrl] of resources) {
                // Prefix all paths with /workspace
                const workspacePath = 'workspace/' + virtualPath

                this.#log(`Loading resource: ${workspacePath} from ${assetUrl}`)
                this.#dispatchProgress(`Loading ${virtualPath}...`, Math.floor((loadedCount / totalResources) * 100))

                const response = await fetch(assetUrl)
                if (!response.ok) {
                    throw new Error(`Failed to fetch ${virtualPath}: ${response.statusText}`)
                }

                const buffer = await response.arrayBuffer()
                const uint8Array = new Uint8Array(buffer)

                this.#ensureDirectoryExists(workspacePath)

                this.#phpModule.FS.writeFile('/' + workspacePath, uint8Array)
                this.#log(`Successfully loaded: ${workspacePath} (${uint8Array.length} bytes)`)

                loadedCount++
            }

            this.#resourcesLoaded = true
            this.#log('All resources loaded successfully')
            this.#dispatchResourcesLoaded()
            return true
        } catch (error) {
            this.#logError('Failed to load resources:', error)
            this.#dispatchError('Failed to load resources: ' + error.message)
            return false
        }
    }

    #ensureDirectoryExists(filePath) {
        const parts = filePath.split('/')
        const directories = parts.slice(0, -1)

        let currentPath = ''
        for (const dir of directories) {
            if (!dir) continue

            currentPath += '/' + dir

            try {
                const pathInfo = this.#phpModule.FS.analyzePath(currentPath)
                if (!pathInfo.exists) {
                    this.#log(`Creating directory: ${currentPath}`)
                    this.#phpModule.FS.mkdir(currentPath)
                }
            } catch (error) {
                this.#logError(`Error checking/creating directory ${currentPath}:`, error)
                throw error
            }
        }
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

    #dispatchResourcesLoaded() {
        this.dispatch('resources-loaded')
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
