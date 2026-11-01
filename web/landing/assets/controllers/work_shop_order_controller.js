import { Controller } from "@hotwired/stimulus"

// Anything that is not a UUID in checkout_id leaves the block hidden.
const CHECKOUT_ID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

export default class extends Controller {
    static targets = ["reference"]

    connect() {
        const checkoutId = new URLSearchParams(window.location.search).get("checkout_id")

        if (checkoutId === null || !CHECKOUT_ID.test(checkoutId)) {
            return
        }

        this.referenceTarget.textContent = checkoutId
        this.element.hidden = false
    }
}
