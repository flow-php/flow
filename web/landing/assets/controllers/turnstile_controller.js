import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    #widgetId = null
    #siteKey = null
    #debug = false
    #pendingPromise = null

    connect() {
        this.#debug = this.application.debug
        this.#siteKey = this.element.dataset.turnstileSiteKey
        this.#log('Connecting Turnstile controller with site key:', this.#siteKey)
        this.#initWidget()
    }

    #initWidget() {
        if (typeof turnstile === 'undefined') {
            this.#log('Turnstile API not loaded yet, retrying...')
            setTimeout(() => this.#initWidget(), 100)
            return
        }

        this.#widgetId = turnstile.render('#turnstile-widget', {
            sitekey: this.#siteKey,
            callback: (token) => {
                this.#log('Turnstile token obtained via callback')
                if (this.#pendingPromise) {
                    this.#pendingPromise.resolve(token)
                    this.#pendingPromise = null
                }
            },
            'error-callback': (error) => {
                this.#logError('Turnstile error:', error)
                if (this.#pendingPromise) {
                    this.#pendingPromise.reject(new Error('Turnstile verification failed'))
                    this.#pendingPromise = null
                }
            },
            appearance: 'interaction-only',
            size: 'compact'
        })

        this.#log('Turnstile widget initialized with ID:', this.#widgetId)
    }

    async getToken() {
        return new Promise((resolve, reject) => {
            if (!this.#widgetId) {
                reject(new Error('Turnstile not initialized'))
                return
            }

            this.#log('Executing Turnstile challenge...')

            // Store promise callbacks for widget callback to use
            this.#pendingPromise = { resolve, reject }

            // Trigger the widget
            turnstile.execute(this.#widgetId)
        })
    }

    reset() {
        if (this.#widgetId) {
            this.#log('Resetting Turnstile widget')
            turnstile.reset(this.#widgetId)
        }
    }

    #log(...args) {
        if (this.#debug) {
            console.log('[Turnstile]', ...args)
        }
    }

    #logError(...args) {
        console.error('[Turnstile]', ...args)
    }
}
