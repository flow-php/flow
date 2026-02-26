<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_xml;

require __DIR__ . '/vendor/autoload.php';

$xml = '<?xml version="1.0"?><root><item>value</item></root>';

echo 'Cast XML: ' . type_xml()->cast($xml)->saveXML();
