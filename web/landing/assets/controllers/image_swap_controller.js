import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    #originalSrc

    connect() {
        this.#originalSrc = this.element.src
    }

    mouseEnter() {
        const hoverSrc = this.element.dataset.hoverSrc
        if (hoverSrc) {
            this.element.src = hoverSrc
        }
    }

    mouseLeave() {
        this.element.src = this.#originalSrc
    }
}
