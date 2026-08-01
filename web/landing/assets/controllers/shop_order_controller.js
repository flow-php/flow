import { Controller } from "@hotwired/stimulus"

// Polar appends ?checkout_id=<uuid> to the success URL. Anything that isn't a
// UUID is someone playing with the query string, so we leave the block hidden
// rather than render it.
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
