import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["helpSection"]

    /**
     * Show the help section
     * Can be triggered from anywhere on the page via custom events or direct method calls
     */
    show(event) {
        if (event) {
            event.preventDefault()
        }

        if (this.hasHelpSectionTarget) {
            this.helpSectionTarget.style.display = 'block'
        }
    }

    /**
     * Hide the help section
     */
    close(event) {
        if (event) {
            event.preventDefault()
        }

        if (this.hasHelpSectionTarget) {
            this.helpSectionTarget.style.display = 'none'
        }
    }

    /**
     * Show a specific help topic by ID
     * @param {Event} event - The triggering event
     */
    showTopic(event) {
        if (event) {
            event.preventDefault()

            const topicId = event.currentTarget.dataset.helpTopic
            if (topicId) {
                this.show()

                // Scroll to the specific topic
                const topicElement = document.getElementById(topicId)
                if (topicElement) {
                    topicElement.scrollIntoView({ behavior: 'smooth', block: 'start' })
                }
            }
        }
    }
}
