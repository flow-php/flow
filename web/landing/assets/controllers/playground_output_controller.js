import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["container"]

    show(message) {
        if (!this.hasContainerTarget) return

        this.containerTarget.innerHTML = this.#render(message)
        this.dispatch('shown', { detail: { message: message.content, type: message.type }, bubbles: true })
    }

    append(message) {
        if (!this.hasContainerTarget) return

        this.containerTarget.innerHTML += '\n' + this.#render(message)
        this.dispatch('appended', { detail: { message: message.content, type: message.type }, bubbles: true })
    }

    clear() {
        if (!this.hasContainerTarget) return

        this.containerTarget.innerHTML = ''
        this.dispatch('cleared', { detail: { timestamp: Date.now() }, bubbles: true })
    }

    isLoaded() {
        return true
    }

    async onLoad() {
        return Promise.resolve()
    }

    #render(message) {
        const timestamp = new Date(message.timestamp || Date.now()).toLocaleTimeString()
        const prefix = this.#getPrefix(message.type)
        const colorClass = message.color ? `output-${message.color}` : message.type ? `output-${message.type}` : 'output-default'

        return `<div class="output-message ${colorClass}"><span class="output-timestamp">${timestamp}</span>${prefix ? `<span class="output-prefix">${prefix}</span>` : ''}<span class="output-content">${this.#escape(message.content)}</span></div>`
    }

    #getPrefix(type) {
        const prefixes = {
            info: '[INFO]',
            success: '[SUCCESS]',
            warning: '[WARNING]',
            error: '[ERROR]'
        }
        return prefixes[type] || ''
    }

    #escape(text) {
        const div = document.createElement('div')
        div.textContent = text
        return div.innerHTML
    }
}
