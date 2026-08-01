import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["link"]

    connect() {
        this.observer = new IntersectionObserver(
            (entries) => {
                entries.forEach((entry) => {
                    if (entry.isIntersecting) {
                        this.#activate(entry.target.id)
                    }
                })
            },
            { rootMargin: "-45% 0px -50% 0px", threshold: 0 },
        )

        this.linkTargets.forEach((link) => {
            const section = document.getElementById(link.dataset.section)

            if (section) {
                this.observer.observe(section)
            }
        })
    }

    disconnect() {
        if (this.observer) {
            this.observer.disconnect()
        }
    }

    #activate(id) {
        this.linkTargets.forEach((link) => {
            if (link.dataset.section === id) {
                link.setAttribute("aria-current", "true")
            } else {
                link.removeAttribute("aria-current")
            }
        })
    }
}
