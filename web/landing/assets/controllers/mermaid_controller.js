import { Controller } from "@hotwired/stimulus";
import Panzoom from '@panzoom/panzoom';

export default class extends Controller {
    static targets = ['svg', 'zoomIn', 'zoomOut'];

    #originalSource = null;
    #onThemeChanged = null;
    #panzoom = null;

    connect() {
        this.#originalSource = this.svgTarget.textContent;
        this.#render();

        this.#onThemeChanged = this.#handleThemeChange.bind(this);
        document.addEventListener('theme:changed', this.#onThemeChanged);
    }

    disconnect() {
        if (this.#onThemeChanged) {
            document.removeEventListener('theme:changed', this.#onThemeChanged);
        }
    }

    #handleThemeChange(event) {
        const resolved = event.detail?.resolved || 'light';
        mermaid.initialize({
            startOnLoad: false,
            theme: resolved === 'dark' ? 'dark' : 'default',
            securityLevel: 'loose',
            flowchart: { useMaxWidth: true, htmlLabels: true },
        });

        this.svgTarget.removeAttribute('data-processed');
        this.svgTarget.innerHTML = '';
        this.svgTarget.textContent = this.#originalSource;
        this.#render();
    }

    #render() {
        mermaid.run({
            nodes: [this.svgTarget],
            postRenderCallback: () => {
                this.#panzoom = Panzoom(this.svgTarget, {});
                this.#panzoom.pan(0, 0);
                this.element.addEventListener('wheel', this.#panzoom.zoomWithWheel);

                this.zoomInTarget.addEventListener('click', (event) => {
                    event.preventDefault();
                    this.#panzoom.zoomIn();
                });

                this.zoomOutTarget.addEventListener('click', (event) => {
                    event.preventDefault();
                    this.#panzoom.zoomOut();
                });
            },
        });
    }
}
