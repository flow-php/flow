<?php

return [
    'app' => [
        'path' => 'app.js',
        'entrypoint' => true,
    ],
    '@symfony/stimulus-bundle' => [
        'path' => '@symfony/stimulus-bundle/loader.js',
    ],
    '@hotwired/stimulus' => [
        'version' => '3.2.1',
    ],
    'highlight.js/lib/core' => [
        'version' => '11.9.0',
    ],
    'highlight.js/lib/languages/php' => [
        'version' => '11.9.0',
    ],
    'highlight.js/styles/github-dark.min.css' => [
        'version' => '11.9.0',
        'type' => 'css',
    ],
    '@fontsource-variable/cabin/index.min.css' => [
        'version' => '5.0.17',
        'type' => 'css',
    ],

    /**
     * On mobile there is a collapsible menu that uses relatively new popover attribute,
     * but it's not yet available in a firefox browser: https://caniuse.com/?search=popover.
     * This polyfill will make it work there.
     *
     * Once it's available, run 'bin/console importmap:remove @oddbird/popover-polyfill'
     * and remove import from 'landing/assets/app.js'.
     */
    '@oddbird/popover-polyfill' => [
        'version' => '0.3.8',
    ],
];
