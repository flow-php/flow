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
];
