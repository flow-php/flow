import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["wasm", "code-editor", "turnstile", "playground-output", "playground-tabs"]
    static targets = ["loadingMessage", "loadingBar", "loadingPercent", "navigation", "editor", "outputContainer", "storageIndicator", "actionSpinner"]
    static values = {
        packageIcon: String,
        linkIcon: String
    }

    connect() {
        this.#log('Connecting playground controller')
    }

    onNotification(event) {
        const { message, type } = event.detail
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: message, type })
        }
    }

    onActionStarted() {
        if (this.hasPlaygroundTabsOutlet) {
            this.playgroundTabsOutlet.switchToCode()
        }
        document.querySelectorAll('#action-run, #action-format, #action-share, #action-upload').forEach(btn => btn.disabled = true)
        const resetLink = document.getElementById('action-reset')
        if (resetLink) {
            resetLink.classList.add('disabled')
        }
        if (this.hasActionSpinnerTarget) {
            this.actionSpinnerTarget.classList.remove('hidden')
        }
    }

    onActionFinished() {
        document.querySelectorAll('#action-run, #action-format, #action-share, #action-upload').forEach(btn => btn.disabled = false)
        const resetLink = document.getElementById('action-reset')
        if (resetLink) {
            resetLink.classList.remove('disabled')
        }
        if (this.hasActionSpinnerTarget) {
            this.actionSpinnerTarget.classList.add('hidden')
        }
    }

    onStorageLoaded(event) {
        this.#log('Code loaded from local storage')
        this.#showIndicator('storage')
    }

    onUrlLoaded(event) {
        this.#log('Code loaded from URL')
        this.#showIndicator('url')
    }

    onDatasetsLoaded(event) {
        this.#log('Datasets loaded from R2')
    }

    onWasmInitialized(event) {
        this.#log('WASM initialized successfully')
        this.hideLoading()
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: 'Click "Run" to execute your code.', type: 'info' })
        }
    }

    onWasmLoadingProgress(event) {
        const { message, percent } = event.detail
        this.showLoading(message, percent)
    }

    onWasmOutput(event) {
        const { output } = event.detail
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: output, type: 'success' })
        }
    }

    onWasmError(event) {
        const { error, errorInfo } = event.detail
        this.#log('Error event received:', { error, errorInfo })
        this.hideLoading()
        this.#showContentAfterLoading()
        if (this.hasPlaygroundOutputOutlet) {
            this.playgroundOutputOutlet.show({ content: error, type: 'error' })
        }

        if (errorInfo && this.hasCodeEditorOutlet) {
            this.#log('Highlighting error:', errorInfo)
            this.codeEditorOutlet.highlightError(errorInfo)
        }
    }

    showLoading(message, percent) {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = message
        }
        if (this.hasLoadingPercentTarget && percent !== undefined) {
            this.loadingPercentTarget.textContent = Math.round(percent) + '%'
        }
        if (this.hasLoadingBarTarget && percent !== undefined) {
            this.loadingBarTarget.style.width = percent + '%'
        }
    }

    hideLoading() {
        if (this.hasLoadingMessageTarget) {
            this.loadingMessageTarget.textContent = ''
        }
        if (this.hasLoadingPercentTarget) {
            this.loadingPercentTarget.textContent = ''
        }
        if (this.hasLoadingBarTarget) {
            this.loadingBarTarget.style.width = '0%'
        }
        this.#showContentAfterLoading()
    }

    isLoaded() {
        return (
            this.hasWasmOutlet && this.wasmOutlet.isLoaded() &&
            this.hasCodeEditorOutlet && this.codeEditorOutlet.isLoaded() &&
            this.hasTurnstileOutlet && this.turnstileOutlet.isLoaded() &&
            this.hasPlaygroundOutputOutlet && this.playgroundOutputOutlet.isLoaded()
        )
    }

    async onLoad() {
        const promises = []

        if (this.hasWasmOutlet) promises.push(this.wasmOutlet.onLoad())
        if (this.hasCodeEditorOutlet) promises.push(this.codeEditorOutlet.onLoad())
        if (this.hasTurnstileOutlet) promises.push(this.turnstileOutlet.onLoad())
        if (this.hasPlaygroundOutputOutlet) promises.push(this.playgroundOutputOutlet.onLoad())

        await Promise.all(promises)
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

    #showIndicator(source) {
        if (!this.hasStorageIndicatorTarget) {
            return
        }

        const icons = {
            storage: `<img src="${this.packageIconValue}" width="16" height="16" alt="">`,
            url: `<img src="${this.linkIconValue}" width="16" height="16" alt="">`
        }

        const labels = {
            storage: 'Loaded from local storage',
            url: 'Loaded from snippet'
        }

        this.storageIndicatorTarget.innerHTML = icons[source] + '<span>' + labels[source] + '</span>'
        this.storageIndicatorTarget.style.display = 'flex'
    }

    #log(...args) {
        if (this.application.debug) {
            console.log('[Playground]', ...args)
        }
    }
}
