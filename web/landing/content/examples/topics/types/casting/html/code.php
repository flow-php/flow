<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_html;

require __DIR__ . '/vendor/autoload.php';

$html = '<html><body><p>Hello World</p></body></html>';

echo 'Cast HTML: ' . type_html()->cast($html)->saveHTML();
