import { Controller } from "@hotwired/stimulus";
import Panzoom from '@panzoom/panzoom';

/* stimulusFetch: 'lazy' */
export default class extends Controller {
    static targets = ['svg', 'zoomIn', 'zoomOut'];

    connect() {
        mermaid.run({
            nodes: [this.svgTarget],
            postRenderCallback: (id) => {
                let panzoom = Panzoom(this.svgTarget, {});
                panzoom.pan(0, 0)
                this.element.addEventListener('wheel', panzoom.zoomWithWheel)

                this.zoomInTarget.addEventListener('click', (event) => {
                    event.preventDefault();
                    panzoom.zoomIn();
                });

                this.zoomOutTarget.addEventListener('click', (event) => {
                    event.preventDefault();
                    panzoom.zoomOut();
                });
            }
        });
    }
}