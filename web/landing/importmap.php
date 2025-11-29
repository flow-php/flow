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
        'version' => '3.2.2',
    ],
    '@fontsource-variable/cabin/index.min.css' => [
        'version' => '5.2.8',
        'type' => 'css',
    ],
    'htmx.org' => [
        'version' => '2.0.8',
    ],
    'clipboard' => [
        'version' => '2.0.11',
    ],
    'prismjs' => [
        'version' => '1.30.0',
    ],
    'prismjs/themes/prism.min.css' => [
        'version' => '1.30.0',
        'type' => 'css',
    ],
    'prismjs/themes/prism-okaidia.min.css' => [
        'version' => '1.30.0',
        'type' => 'css',
    ],
    'prismjs/plugins/autoloader/prism-autoloader.js' => [
        'version' => '1.30.0',
    ],
    'prismjs/components/prism-bash.min.js' => [
        'version' => '1.30.0',
    ],
    'prismjs/components/prism-markup-templating.min.js' => [
        'version' => '1.30.0',
    ],
    'prismjs/components/prism-php.min.js' => [
        'version' => '1.30.0',
    ],
    'prismjs/components/prism-json.min.js' => [
        'version' => '1.30.0',
    ],
    'flowbite' => [
        'version' => '3.1.2',
    ],
    '@popperjs/core' => [
        'version' => '2.11.8',
    ],
    'flowbite-datepicker' => [
        'version' => '1.3.2',
    ],
    'flowbite/dist/flowbite.min.css' => [
        'version' => '3.1.2',
        'type' => 'css',
    ],
    'prismjs/components/prism-csv.min.js' => [
        'version' => '1.30.0',
    ],
    '@panzoom/panzoom' => [
        'version' => '4.6.0',
    ],
    'lz-string' => [
        'version' => '1.5.0',
    ],
    'codemirror' => [
        'version' => '6.0.2',
    ],
    '@codemirror/view' => [
        'version' => '6.38.6',
    ],
    '@codemirror/state' => [
        'version' => '6.5.2',
    ],
    '@codemirror/language' => [
        'version' => '6.11.3',
    ],
    '@codemirror/commands' => [
        'version' => '6.10.0',
    ],
    '@codemirror/search' => [
        'version' => '6.5.11',
    ],
    '@codemirror/autocomplete' => [
        'version' => '6.19.1',
    ],
    '@codemirror/lint' => [
        'version' => '6.9.2',
    ],
    'style-mod' => [
        'version' => '4.1.3',
    ],
    'w3c-keyname' => [
        'version' => '2.2.8',
    ],
    'crelt' => [
        'version' => '1.0.6',
    ],
    '@marijn/find-cluster-break' => [
        'version' => '1.0.2',
    ],
    '@lezer/common' => [
        'version' => '1.3.0',
    ],
    '@lezer/highlight' => [
        'version' => '1.2.3',
    ],
    '@codemirror/lang-php' => [
        'version' => '6.0.2',
    ],
    '@lezer/php' => [
        'version' => '1.0.5',
    ],
    '@codemirror/lang-html' => [
        'version' => '6.4.11',
    ],
    '@lezer/lr' => [
        'version' => '1.4.3',
    ],
    '@lezer/html' => [
        'version' => '1.3.12',
    ],
    '@codemirror/lang-css' => [
        'version' => '6.3.1',
    ],
    '@codemirror/lang-javascript' => [
        'version' => '6.2.4',
    ],
    '@lezer/css' => [
        'version' => '1.3.0',
    ],
    '@lezer/javascript' => [
        'version' => '1.5.4',
    ],
    '@codemirror/theme-one-dark' => [
        'version' => '6.1.3',
    ],
    '@codemirror/lang-json' => [
        'version' => '6.0.1',
    ],
    '@lezer/json' => [
        'version' => '1.0.3',
    ],
    '@codemirror/lang-xml' => [
        'version' => '6.1.0',
    ],
    '@lezer/xml' => [
        'version' => '1.0.6',
    ],
];
