<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_html;

require __DIR__ . '/vendor/autoload.php';

$html = '<html><body><p>Hello World</p></body></html>';

echo 'Assert HTML: ' . type_html()->assert($html)->saveHTML();
