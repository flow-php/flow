import { Controller } from "@hotwired/stimulus"

// Brand palette from tailwind.config.js, plus white so the fall reads on the dark theme.
const COLORS = ["#ff5547", "#806dfe", "#e1761a", "#5945d8", "#ffffff"]
const PIECES = 150
const GRAVITY = 0.04
const MAX_FALL = 3.5
// Pieces start staggered above the fold so they keep arriving instead of landing all at once.
const STAGGER = 1.1
const FADE_FROM = 0.82

export default class extends Controller {
    connect() {
        this.motion = window.matchMedia("(prefers-reduced-motion: reduce)")
        this.pieces = []
        this.frame = null
        this.onResize = () => this.#resize()

        this.burst()
    }

    disconnect() {
        this.#stop()
        this.#unmount()
    }

    burst() {
        if (this.motion.matches) {
            return
        }

        this.#mount()

        const width = window.innerWidth
        const height = window.innerHeight

        for (let i = 0; i < PIECES; i++) {
            this.pieces.push({
                x: Math.random() * width,
                y: -20 - Math.random() * height * STAGGER,
                vx: (Math.random() - 0.5) * 0.7,
                vy: 1 + Math.random() * 1.5,
                sway: 0.4 + Math.random() * 1.1,
                swaySpeed: 0.01 + Math.random() * 0.03,
                phase: Math.random() * Math.PI * 2,
                width: 5 + Math.random() * 5,
                height: 9 + Math.random() * 6,
                rotation: Math.random() * Math.PI * 2,
                spin: (Math.random() - 0.5) * 0.12,
                flip: Math.random() * Math.PI * 2,
                flipSpeed: 0.05 + Math.random() * 0.09,
                color: COLORS[Math.floor(Math.random() * COLORS.length)],
            })
        }

        this.#start()
    }

    #tick() {
        const context = this.context
        const height = window.innerHeight

        context.clearRect(0, 0, window.innerWidth, height)

        this.pieces = this.pieces.filter((piece) => {
            piece.vy = Math.min(piece.vy + GRAVITY, MAX_FALL)
            piece.phase += piece.swaySpeed
            piece.y += piece.vy
            piece.x += piece.vx + Math.sin(piece.phase) * piece.sway
            piece.rotation += piece.spin
            piece.flip += piece.flipSpeed

            if (piece.y > height + 40) {
                return false
            }

            const travelled = piece.y / height

            context.save()
            context.translate(piece.x, piece.y)
            context.rotate(piece.rotation)
            // Squashing the height as it tumbles fakes a flat piece turning over.
            context.scale(1, Math.abs(Math.cos(piece.flip)))
            context.globalAlpha = travelled > FADE_FROM ? Math.max(0, 1 - (travelled - FADE_FROM) / (1 - FADE_FROM)) : 1
            context.fillStyle = piece.color
            context.fillRect(-piece.width / 2, -piece.height / 2, piece.width, piece.height)
            context.restore()

            return true
        })

        if (this.pieces.length === 0) {
            this.#stop()
            this.#unmount()

            return
        }

        this.frame = window.requestAnimationFrame(() => this.#tick())
    }

    #mount() {
        if (this.canvas) {
            return
        }

        this.canvas = document.createElement("canvas")
        this.canvas.setAttribute("aria-hidden", "true")
        this.canvas.style.cssText = "position:fixed;inset:0;pointer-events:none;z-index:60"
        document.body.appendChild(this.canvas)
        this.context = this.canvas.getContext("2d")

        this.#resize()
        window.addEventListener("resize", this.onResize)
    }

    #resize() {
        if (!this.canvas) {
            return
        }

        const ratio = window.devicePixelRatio || 1

        this.canvas.width = window.innerWidth * ratio
        this.canvas.height = window.innerHeight * ratio
        this.canvas.style.width = `${window.innerWidth}px`
        this.canvas.style.height = `${window.innerHeight}px`
        this.context.setTransform(ratio, 0, 0, ratio, 0, 0)
    }

    #unmount() {
        window.removeEventListener("resize", this.onResize)

        if (this.canvas) {
            this.canvas.remove()
            this.canvas = null
            this.context = null
        }
    }

    #start() {
        if (this.frame === null) {
            this.frame = window.requestAnimationFrame(() => this.#tick())
        }
    }

    #stop() {
        if (this.frame !== null) {
            window.cancelAnimationFrame(this.frame)
            this.frame = null
        }
    }
}
