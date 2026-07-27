import { Controller } from "@hotwired/stimulus"

const RECYCLE_THRESHOLD_BYTES = 1536 * 1024 * 1024

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
    #recycleCount = 0

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

    #formatCode(code, callback) {
        if (!this.#phpModuleLoaded) {
            callback(null, 'PHP module not loaded yet', null)
            return
        }

        this.#combinedOutput = ''

        try {
            this.#phpModule.FS.chdir('/workspace')

            const codeFile = '/workspace/code.php'

            this.#phpModule.FS.writeFile(codeFile, code)

            const formatScript = `<?php
\$argc = 2;
\$argv = ['cs-fixer.php', '${codeFile}'];
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
                    const formattedCode = this.#phpModule.FS.readFile(codeFile, { encoding: 'utf8' })

                    const appliedFixers = this.#parseAppliedFixers(output)

                    callback(formattedCode, null, appliedFixers)
                } catch (readError) {
                    callback(null, 'Failed to read formatted code: ' + readError.message, null)
                }
            } else {
                callback(null, 'Unexpected output from formatter: ' + output, null)
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

    isReady() {
        return this.#phpModuleLoaded
    }

    isLoaded() {
        return this.#phpModuleLoaded
    }

    async onLoad() {
        if (this.isLoaded()) return Promise.resolve()

        return new Promise(resolve => {
            const check = setInterval(() => {
                if (this.isLoaded()) {
                    clearInterval(check)
                    resolve()
                }
            }, 50)
        })
    }

    areResourcesLoaded() {
        return this.#resourcesLoaded
    }

    getModule() {
        return this.#phpModule
    }

    async run(code) {
        if (!this.#phpModuleLoaded) {
            throw new Error('PHP module not loaded')
        }

        await this.#recycleIfNeeded()

        this.#combinedOutput = ''

        try {
            this.#phpModule.FS.chdir('/workspace')

            const exitCode = this.#phpModule.ccall('pib_eval', 'number', ['string'], [code])

            const errorInfo = this.#parseErrorMessage(this.#combinedOutput)

            if (errorInfo) {
                return {
                    success: false,
                    error: {
                        type: errorInfo.type,
                        message: this.#combinedOutput,
                        line: errorInfo.line,
                        column: errorInfo.column,
                        severity: 'error'
                    },
                    output: this.#combinedOutput,
                    exitCode,
                    executionTime: 0,
                    memoryUsed: 0
                }
            }

            return {
                success: true,
                output: this.#combinedOutput || 'Code executed successfully (no output)',
                exitCode,
                executionTime: 0,
                memoryUsed: 0
            }
        } catch (e) {
            return {
                success: false,
                error: {
                    type: 'Exception',
                    message: e.message,
                    severity: 'error'
                },
                output: this.#combinedOutput,
                exitCode: 1,
                executionTime: 0,
                memoryUsed: 0
            }
        }
    }

    async format(code) {
        await this.#recycleIfNeeded()

        return new Promise((resolve) => {
            this.#formatCode(code, (formattedCode, error, appliedFixers) => {
                if (error) {
                    resolve({ success: false, error })
                } else {
                    resolve({ success: true, code: formattedCode, fixers: appliedFixers })
                }
            })
        })
    }

    async readFile(path, binary = false) {
        const content = binary ? this.#readFileBinarySync(path) : this.#readFileSync(path)
        if (content === null) {
            return { success: false, error: 'File not found' }
        }
        return { success: true, content }
    }

    async writeFile(path, content) {
        if (!this.#phpModuleLoaded) {
            return { success: false, error: 'PHP module not loaded yet', bytesWritten: 0 }
        }

        try {
            const FS = this.#phpModule.FS
            const fileExists = this.#fileExistsSync(path)

            const pathParts = path.split('/')
            const dirPath = pathParts.slice(0, -1).join('/')
            if (dirPath) {
                this.#ensureDirectoryExists(path)
            }

            const uint8Array = typeof content === 'string'
                ? new TextEncoder().encode(content)
                : content

            FS.writeFile(path, uint8Array)

            const eventName = fileExists ? 'file-updated' : 'file-created'
            this.dispatch(eventName, {
                detail: { path, size: uint8Array.length },
                bubbles: true
            })

            return { success: true, bytesWritten: uint8Array.length }
        } catch (error) {
            this.#logError(`Error writing file ${path}:`, error)
            return { success: false, error: error.message, bytesWritten: 0 }
        }
    }

    #fileExistsSync(path) {
        try {
            const stat = this.#phpModule.FS.stat(path)
            return stat !== null
        } catch (error) {
            return false
        }
    }

    async listFiles(path = '/workspace', recursive = false) {
        const files = this.#listFilesSync(path, recursive)
        return { success: true, files }
    }

    async removeFile(path) {
        try {
            this.#phpModule.FS.unlink(path)
            this.dispatch('file-deleted', { detail: { path }, bubbles: true })
            return { success: true }
        } catch (error) {
            return { success: false, error: error.message }
        }
    }

    #listFilesSync(path = '/', recursive = false) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot list files: PHP module not loaded yet')
            return []
        }

        try {
            const files = []
            const FS = this.#phpModule.FS

            const readDirectory = (dirPath, shouldRecurse) => {
                try {
                    const entries = FS.readdir(dirPath)

                    for (const entry of entries) {
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
                                if (shouldRecurse) {
                                    readDirectory(fullPath, true)
                                }
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

            readDirectory(path, recursive)
            return files
        } catch (error) {
            this.#logError('Error listing files:', error)
            return []
        }
    }

    #readFileSync(filePath) {
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

    #readFileBinarySync(filePath) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot read file: PHP module not loaded yet')
            return null
        }

        try {
            const FS = this.#phpModule.FS
            const content = FS.readFile(filePath)
            this.#log('File read successfully (binary):', filePath)
            return content
        } catch (error) {
            this.#logError('Error reading file:', error)
            return null
        }
    }

    #uploadFile(filename, uint8Array) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot upload file: PHP module not loaded yet')
            return false
        }

        try {
            const filePath = '/workspace/uploads/' + filename
            this.#log(`Writing file to WASM FS: ${filePath} (${uint8Array.length} bytes)`)
            this.#phpModule.FS.writeFile(filePath, uint8Array)
            this.#log(`Successfully uploaded: ${filePath}`)

            return true
        } catch (error) {
            this.#logError(`Error uploading file ${filename}:`, error)
            this.#logError('Error details:', error.message, 'errno:', error.errno)
            return false
        }
    }

    // announceReady=false while recycling: wasm:initialized makes playground-storage reload code.php
    // from the example's initial code, which would throw away the user's edits.
    async #loadResources(announceReady = true) {
        if (!this.#phpModuleLoaded) {
            this.#logError('Cannot load resources: PHP module not loaded yet')
            return false
        }

        if (this.#resourcesLoaded) {
            this.#log('Resources already loaded')
            return true
        }

        this.#log('Loading resources into WASM filesystem...', this.resourcesValue)

        try {
            this.#log('Creating /workspace directory')
            this.#phpModule.FS.mkdir('/workspace')

            this.#log('Creating /workspace/uploads directory')
            this.#phpModule.FS.mkdir('/workspace/uploads')

            // Examples write results under output/. Flow's filesystem creates missing directories
            // on write, but PDO/sqlite does not, so the directory has to exist up front.
            this.#log('Creating /workspace/output directory')
            this.#phpModule.FS.mkdir('/workspace/output')

            if (!this.resourcesValue || Object.keys(this.resourcesValue).length === 0) {
                this.#log('No resources configured to load')
                this.#resourcesLoaded = true
                if (announceReady) {
                    this.#dispatchResourcesLoaded()
                    this.#dispatchInitialized()
                }
                return true
            }

            const resources = Object.entries(this.resourcesValue)
            const totalResources = resources.length
            let loadedCount = 0

            for (const [virtualPath, assetUrl] of resources) {
                // Prefix all paths with /workspace
                const workspacePath = 'workspace/' + virtualPath

                this.#log(`Loading resource: ${workspacePath} from ${assetUrl}`)
                this.#dispatchLoadingProgress(`Loading ${virtualPath}...`, Math.floor((loadedCount / totalResources) * 100))

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
            if (announceReady) {
                this.#dispatchResourcesLoaded()
                this.#dispatchInitialized()
            }
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

        const oldScript = document.getElementById('php-wasm-script')
        if (oldScript) {
            oldScript.remove()
        }

        this.#phpModuleLoaded = false
        this.#phpModule = null

        const script = document.createElement('script')
        script.id = 'php-wasm-script'
        script.src = this.phpJsValue

        script.onload = () => {
            this.#log('PHP script loaded, initializing module...')
            this.#dispatchProgress('Initializing PHP runtime...', 30)

            if (typeof PHP !== 'undefined') {
                this.#instantiate().then(async () => {
                    this.#log('PHP module initialized successfully')

                    await this.#loadResources()
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

    // Reused by the recycling path, which must not re-inject the <script>: PHP is already global.
    async #instantiate() {
        this.#phpModuleLoaded = false
        this.#phpModule = null

        const module = await PHP({
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
        })

        this.#phpModule = module
        this.#phpModuleLoaded = true
    }

    #heapBytes() {
        try {
            return this.#phpModule.ccall('pib_heap_bytes', 'number', [], [])
        } catch (error) {
            this.#logError('pib_heap_bytes unavailable:', error)
            return 0
        }
    }

    // Runs before the next Run, not after a failed one: past the wall the module is unrecoverable.
    async #recycleIfNeeded() {
        if (!this.#phpModuleLoaded) {
            return
        }

        const bytes = this.#heapBytes()
        if (bytes < RECYCLE_THRESHOLD_BYTES) {
            return
        }

        this.#log(`Heap at ${Math.round(bytes / 1048576)} MB, recycling PHP module`)
        this.#dispatchProgress('Reclaiming memory...', 0)

        try {
            const workspace = this.#snapshotWorkspace()
            this.#resourcesLoaded = false

            await this.#instantiate()
            await this.#loadResources(false)

            // After #loadResources, so user edits win over the pristine copies it re-fetches.
            this.#restoreWorkspace(workspace)

            this.#recycleCount++
            this.#log(`PHP module recycled (${this.#recycleCount}), heap now ${Math.round(this.#heapBytes() / 1048576)} MB`)
        } catch (error) {
            this.#logError('Failed to recycle PHP module:', error)
            this.#phpModuleLoaded = false
            this.#dispatchError('Failed to reclaim memory: ' + error.message)
        }
    }

    // Everything under /workspace, so uploads, generated output and the user's code survive a recycle.
    #snapshotWorkspace() {
        const files = []
        const walk = (dir) => {
            for (const entry of this.#phpModule.FS.readdir(dir)) {
                if (entry === '.' || entry === '..') {
                    continue
                }

                const path = dir + '/' + entry
                try {
                    const stat = this.#phpModule.FS.stat(path)
                    if (this.#phpModule.FS.isDir(stat.mode)) {
                        walk(path)
                    } else {
                        files.push({ path, bytes: this.#phpModule.FS.readFile(path) })
                    }
                } catch (error) {
                    this.#logError(`Skipping ${path} while snapshotting workspace:`, error)
                }
            }
        }

        walk('/workspace')
        this.#log(`Snapshotted ${files.length} workspace files`)

        return files
    }

    #restoreWorkspace(files) {
        for (const { path, bytes } of files) {
            try {
                this.#ensureDirectoryExists(path)
                this.#phpModule.FS.writeFile(path, bytes)
            } catch (error) {
                this.#logError(`Failed to restore ${path}:`, error)
            }
        }

        this.#log(`Restored ${files.length} workspace files`)
    }

    #dispatchReady() {
        this.dispatch('ready')
    }

    #dispatchInitialized() {
        this.dispatch('initialized')
    }

    #dispatchResourcesLoaded() {
        this.dispatch('resources-loaded')
    }

    #dispatchLoadingProgress(message, percent) {
        this.dispatch('loading-progress', { detail: { message, percent } })
    }

    #dispatchProgress(message, percent) {
        this.dispatch('progress', { detail: { message, percent } })
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
