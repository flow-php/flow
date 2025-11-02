import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    confirmReset(event) {
        if (!confirm('Are you sure you want to reset the playground? Any unsaved changes will be lost.')) {
            event.preventDefault()
        }
    }
}
