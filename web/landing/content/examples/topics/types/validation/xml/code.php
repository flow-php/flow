<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_xml;

require __DIR__ . '/vendor/autoload.php';

$validXml = '<?xml version="1.0"?><root><item>value</item></root>';
$invalidXml = '<root><unclosed>';

echo 'Is valid XML valid? ' . (type_xml()->isValid($validXml) ? 'yes' : 'no') . "\n";
echo 'Is invalid XML valid? ' . (type_xml()->isValid($invalidXml) ? 'yes' : 'no') . "\n";
echo 'Is DOMDocument valid? ' . (type_xml()->isValid(new \DOMDocument()) ? 'yes' : 'no') . "\n";
echo 'Is empty string valid? ' . (type_xml()->isValid('') ? 'yes' : 'no') . "\n";
