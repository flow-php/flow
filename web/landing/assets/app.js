import '@oddbird/popover-polyfill';
import './bootstrap.js';
import 'htmx.org'
import 'flowbite';
import Panzoom from '@panzoom/panzoom';

window.addEventListener('DOMContentLoaded', () => {
    // mermaid.run({
    //     querySelector: '.mermaid',
    //     postRenderCallback: (id) => {
    //         let svgElement = window.document.getElementById(id);
    //         let panzoom = Panzoom(svgElement, {});
    //         panzoom.pan(0, 0)
    //         svgElement.parentElement.addEventListener('wheel', panzoom.zoomWithWheel)
    //
    //         let zoomInElement = svgElement.parentElement.parentElement.querySelector('[data-zoom-in]');
    //         let zoomOutElemetn = svgElement.parentElement.parentElement.querySelector('[data-zoom-out]');
    //
    //         zoomInElement.addEventListener('click', (event) => {
    //             event.preventDefault();
    //             panzoom.zoomIn();
    //         });
    //
    //         zoomOutElemetn.addEventListener('click', (event) => {
    //             event.preventDefault();
    //             panzoom.zoomOut();
    //         });
    //     }
    // });
});
