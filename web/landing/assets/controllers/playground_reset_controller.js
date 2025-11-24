import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static outlets = ["playground-storage"]

    confirmReset(event) {
        if (!confirm('Are you sure you want to reset the playground? Any unsaved changes will be lost.')) {
            event.preventDefault()
        } else {
            if (this.hasPlaygroundStorageOutlet) {
                this.playgroundStorageOutlet.clearCode()
            }
        }
    }
}
