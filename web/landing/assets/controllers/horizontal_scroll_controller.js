import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["container", "leftArrow", "rightArrow"]

    #scrollAmount = 200

    connect() {
        this.#updateArrowVisibility()
        this.boundUpdateVisibility = this.#updateArrowVisibility.bind(this)
        this.containerTarget.addEventListener('scroll', this.boundUpdateVisibility)
        window.addEventListener('resize', this.boundUpdateVisibility)
    }

    disconnect() {
        this.containerTarget.removeEventListener('scroll', this.boundUpdateVisibility)
        window.removeEventListener('resize', this.boundUpdateVisibility)
    }

    scrollLeft(event) {
        event.preventDefault()
        this.containerTarget.scrollBy({
            left: -this.#scrollAmount,
            behavior: 'smooth'
        })
    }

    scrollRight(event) {
        event.preventDefault()
        this.containerTarget.scrollBy({
            left: this.#scrollAmount,
            behavior: 'smooth'
        })
    }

    #updateArrowVisibility() {
        const container = this.containerTarget
        const canScrollLeft = container.scrollLeft > 0
        const canScrollRight = container.scrollLeft < (container.scrollWidth - container.clientWidth - 1)

        if (this.hasLeftArrowTarget) {
            this.leftArrowTarget.classList.toggle('opacity-0', !canScrollLeft)
            this.leftArrowTarget.classList.toggle('pointer-events-none', !canScrollLeft)
        }

        if (this.hasRightArrowTarget) {
            this.rightArrowTarget.classList.toggle('opacity-0', !canScrollRight)
            this.rightArrowTarget.classList.toggle('pointer-events-none', !canScrollRight)
        }
    }
}
