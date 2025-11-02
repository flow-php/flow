import { startStimulusApp } from '@symfony/stimulus-bundle';

addEventListener("DOMContentLoaded", (event) => {
    const app = startStimulusApp()

    let env = window.document.body.dataset.environment;

    if (window.location.search.includes('__env=')) {
        env = window.location.search.split('__env=')[1];
    }

    if (env === 'prod') {
        app.debug = false;
    }
})