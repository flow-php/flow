import { Controller } from "@hotwired/stimulus"
import { PolarEmbedCheckout } from "@polar-sh/checkout/embed"

export default class extends Controller {
    static values = { url: String }

    open(event) {
        event.preventDefault()

        const theme = document.documentElement.classList.contains("dark") ? "dark" : "light"

        PolarEmbedCheckout.create(this.urlValue, { theme })
    }
}
