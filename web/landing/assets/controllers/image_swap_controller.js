import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["image"]

    #originalSrc

    connect() {
        const imageElement = this.hasImageTarget ? this.imageTarget : this.element
        this.#originalSrc = imageElement.src
    }

    mouseEnter() {
        const imageElement = this.hasImageTarget ? this.imageTarget : this.element
        const hoverSrc = imageElement.dataset.hoverSrc
        if (hoverSrc) {
            imageElement.src = hoverSrc
        }
    }

    mouseLeave() {
        const imageElement = this.hasImageTarget ? this.imageTarget : this.element
        imageElement.src = this.#originalSrc
    }
}
