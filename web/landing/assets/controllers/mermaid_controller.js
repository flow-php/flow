import { Controller } from "@hotwired/stimulus";
import Panzoom from '@panzoom/panzoom';
import { mermaid } from '../services/mermaid.js';

let sequence = 0;

export default class extends Controller {
    static targets = ['svg', 'zoomIn', 'zoomOut'];
    static values = { src: String };

    #id = `mermaid-diagram-${sequence++}`;
    #source = null;
    #panzoom = null;
    #onZoomIn = null;
    #onZoomOut = null;
    #onWheel = null;
    #onThemeChanged = null;

    connect() {
        this.#source = this.svgTarget.textContent;

        this.#onZoomIn = (event) => {
            event.preventDefault();
            this.#panzoom?.zoomIn();
        };
        this.#onZoomOut = (event) => {
            event.preventDefault();
            this.#panzoom?.zoomOut();
        };
        this.#onWheel = (event) => this.#panzoom?.zoomWithWheel(event);
        this.#onThemeChanged = () => this.#render();

        this.zoomInTarget.addEventListener('click', this.#onZoomIn);
        this.zoomOutTarget.addEventListener('click', this.#onZoomOut);
        this.element.addEventListener('wheel', this.#onWheel);
        document.addEventListener('theme:changed', this.#onThemeChanged);

        this.#render();
    }

    disconnect() {
        this.zoomInTarget.removeEventListener('click', this.#onZoomIn);
        this.zoomOutTarget.removeEventListener('click', this.#onZoomOut);
        this.element.removeEventListener('wheel', this.#onWheel);
        document.removeEventListener('theme:changed', this.#onThemeChanged);

        this.#panzoom?.destroy();
        this.#panzoom = null;
    }

    async #render() {
        this.#panzoom?.destroy();
        this.#panzoom = null;

        try {
            const { svg, bindFunctions } = await (await mermaid(this.srcValue)).render(this.#id, this.#source);

            this.svgTarget.innerHTML = svg;
            bindFunctions?.(this.svgTarget);

            this.#panzoom = Panzoom(this.svgTarget, {});
            this.#panzoom.pan(0, 0);
        } catch (error) {
            this.svgTarget.textContent = this.#source;
            console.error(`Failed to render mermaid diagram "${this.#id}"`, error);
        } finally {
            this.element.dataset.mermaidState = 'ready';
        }
    }
}
