import { startStimulusApp } from '@symfony/stimulus-bundle';

function initializeStimulus() {
    const app = startStimulusApp()

    window.Stimulus = app;

    let env = window.document.body.dataset.environment;

    if (window.location.search.includes('__env=')) {
        env = window.location.search.split('__env=')[1];
    }

    if (env === 'prod') {
        app.debug = false;
    }
}

if (document.readyState === 'loading') {
    document.addEventListener("DOMContentLoaded", initializeStimulus);
} else {
    initializeStimulus();
}