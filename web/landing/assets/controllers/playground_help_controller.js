import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["helpSection"]

    show(event) {
        if (event) {
            event.preventDefault()
        }

        if (this.hasHelpSectionTarget) {
            this.helpSectionTarget.style.display = 'block'
        }
    }

    close(event) {
        if (event) {
            event.preventDefault()
        }

        if (this.hasHelpSectionTarget) {
            this.helpSectionTarget.style.display = 'none'
        }
    }

    showTopic(event) {
        if (event) {
            event.preventDefault()

            const topicId = event.currentTarget.dataset.helpTopic
            if (topicId) {
                const allTopics = document.querySelectorAll('.help-topic')
                allTopics.forEach(topic => {
                    topic.style.display = 'none'
                })

                this.show()

                const topicElement = document.getElementById(topicId)
                if (topicElement) {
                    topicElement.style.display = 'block'
                }
            }
        }
    }
}
