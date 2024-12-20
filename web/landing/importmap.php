<?php

/**
 * Returns the importmap for this application.
 *
 * - "path" is a path inside the asset mapper system. Use the
 *     "debug:asset-map" command to see the full list of paths.
 *
 * - "entrypoint" (JavaScript only) set to true for any module that will
 *     be used as an "entrypoint" (and passed to the importmap() Twig function).
 *
 * The "importmap:require" command can be used to add new entries to this file.
 */
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
    '@fontsource-variable/cabin/index.min.css' => [
        'version' => '5.0.17',
        'type' => 'css',
    ],
    'htmx.org' => [
        'version' => '1.9.10',
    ],
    'clipboard' => [
        'version' => '2.0.11',
    ],
    '@oddbird/popover-polyfill' => [
        'version' => '0.3.8',
    ],
    'prismjs' => [
        'version' => '1.29.0',
    ],
    'prismjs/themes/prism.min.css' => [
        'version' => '1.29.0',
        'type' => 'css',
    ],
    'prismjs/themes/prism-okaidia.min.css' => [
        'version' => '1.29.0',
        'type' => 'css',
    ],
    'prismjs/plugins/autoloader/prism-autoloader.js' => [
        'version' => '1.29.0',
    ],
    'prismjs/components/prism-bash.min.js' => [
        'version' => '1.29.0',
    ],
    'prismjs/components/prism-markup-templating.min.js' => [
        'version' => '1.29.0',
    ],
    'prismjs/components/prism-php.min.js' => [
        'version' => '1.29.0',
    ],
    'prismjs/components/prism-json.min.js' => [
        'version' => '1.29.0',
    ],
    'flowbite' => [
        'version' => '2.5.2',
    ],
    '@popperjs/core' => [
        'version' => '2.11.8',
    ],
    'flowbite-datepicker' => [
        'version' => '1.3.0',
    ],
    'flowbite/dist/flowbite.min.css' => [
        'version' => '2.5.2',
        'type' => 'css',
    ],
];
