<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_html;

require __DIR__ . '/vendor/autoload.php';

$document = \Dom\HTMLDocument::createFromString('<html><body><p>Hello World</p></body></html>', \LIBXML_NOERROR);

echo 'Assert HTML: ' . type_html()->assert($document)->saveHTML();
