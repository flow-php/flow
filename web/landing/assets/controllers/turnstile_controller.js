import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static values = {
        environment: { type: String, default: 'prod' },
        siteKey: String,
        appearance: { type: String, default: 'interaction-only' },
        size: { type: String, default: 'normal' }
    }

    #widgetId = null
    #debug = false
    #pendingPromise = null

    connect() {
        this.#debug = this.application.debug
        this.#log('Connecting Turnstile controller with site key:', this.siteKeyValue, 'environment:', this.environmentValue)

        if (this.environmentValue === 'test') {
            this.#log('Test environment detected - Turnstile widget will be bypassed')
            this.#widgetId = 'test-bypass'
            return
        }

        this.#initWidget()
    }

    #initWidget() {
        if (typeof turnstile === 'undefined') {
            this.#log('Turnstile API not loaded yet, retrying...')
            setTimeout(() => this.#initWidget(), 100)
            return
        }

        this.#widgetId = turnstile.render('#turnstile-widget', {
            sitekey: this.siteKeyValue,
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
            appearance: this.appearanceValue,
            size: this.sizeValue
        })

        this.#log('Turnstile widget initialized with ID:', this.#widgetId, 'appearance:', this.appearanceValue)
    }

    async getToken() {
        if (this.environmentValue === 'test') {
            this.#log('Test environment - returning mock token')
            return Promise.resolve('test-bypass-token')
        }

        return new Promise((resolve, reject) => {
            if (!this.#widgetId) {
                reject(new Error('Turnstile not initialized'))
                return
            }

            this.#log('Executing Turnstile challenge...')

            this.#pendingPromise = { resolve, reject }

            turnstile.execute(this.#widgetId)
        })
    }

    reset() {
        if (this.environmentValue === 'test') {
            this.#log('Test environment - skipping widget reset')
            return
        }

        if (this.#widgetId) {
            this.#log('Resetting Turnstile widget')
            turnstile.reset(this.#widgetId)
        }
    }

    isLoaded() {
        return this.#widgetId !== null
    }

    async onLoad() {
        if (this.isLoaded()) return Promise.resolve()

        return new Promise(resolve => {
            const check = setInterval(() => {
                if (this.isLoaded()) {
                    clearInterval(check)
                    resolve()
                }
            }, 100)
        })
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
