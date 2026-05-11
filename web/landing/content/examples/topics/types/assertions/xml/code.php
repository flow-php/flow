<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_xml;

require __DIR__ . '/vendor/autoload.php';

$document = new \DOMDocument();
$document->loadXML('<?xml version="1.0"?><root><item>value</item></root>');

echo 'Assert XML: ' . type_xml()->assert($document)->saveXML();
