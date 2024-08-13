import { Controller } from "@hotwired/stimulus"

export default class extends Controller {
    connect() {
        this.toggleButton()
        this.boundToggleMethod = this.toggleButton.bind(this)
        window.addEventListener('scroll', this.boundToggleMethod)
        window.addEventListener('resize', this.boundToggleMethod)
    }

    disconnect() {
        window.removeEventListener('scroll', this.boundToggleMethod)
        window.removeEventListener('resize', this.boundToggleMethod)
    }

    toggleButton() {
        if (window.innerWidth < 640) { // 640px is Tailwind's 'sm' breakpoint
            if (window.scrollY > 780) {
                this.element.classList.remove('opacity-0')
                this.element.classList.add('opacity-100')
            } else {
                this.element.classList.remove('opacity-100')
                this.element.classList.add('opacity-0')
            }
        } else {
            this.element.classList.add('opacity-0')
            this.element.classList.remove('opacity-100')
        }
    }

    scrollToTop(event) {
        event.preventDefault()
        window.scrollTo({
            top: 0,
            behavior: 'smooth'
        })
    }
}