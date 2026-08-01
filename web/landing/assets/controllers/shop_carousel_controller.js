import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    static targets = ["track", "slide", "dot"]
    static values = { index: { type: Number, default: 0 } }

    indexValueChanged() {
        const count = this.slideTargets.length

        if (count === 0) {
            return
        }

        if (this.indexValue < 0) {
            this.indexValue = count - 1
            return
        }

        if (this.indexValue > count - 1) {
            this.indexValue = 0
            return
        }

        this.trackTarget.style.transform = `translateX(-${this.indexValue * 100}%)`

        this.slideTargets.forEach((slide, i) => {
            slide.setAttribute('aria-hidden', i === this.indexValue ? 'false' : 'true')
        })

        this.dotTargets.forEach((dot, i) => {
            const active = i === this.indexValue
            dot.classList.toggle('bg-white', active)
            dot.classList.toggle('bg-white/40', !active)
            dot.setAttribute('aria-current', active ? 'true' : 'false')
        })
    }

    next() {
        this.indexValue++
    }

    prev() {
        this.indexValue--
    }

    goTo(event) {
        this.indexValue = Number(event.params.index)
    }

    keydown(event) {
        if (event.key === "ArrowLeft") {
            event.preventDefault()
            this.prev()
        } else if (event.key === "ArrowRight") {
            event.preventDefault()
            this.next()
        }
    }
}
