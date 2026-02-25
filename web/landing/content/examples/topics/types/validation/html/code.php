<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_html;

require __DIR__ . '/vendor/autoload.php';

$validHtml = '<html><body><p>Hello World</p></body></html>';
$simpleHtml = '<p>Simple paragraph</p>';

echo 'Is valid HTML valid? ' . (type_html()->isValid($validHtml) ? 'yes' : 'no') . "\n";
echo 'Is simple HTML valid? ' . (type_html()->isValid($simpleHtml) ? 'yes' : 'no') . "\n";
echo 'Is DOMDocument valid? ' . (type_html()->isValid(new \DOMDocument()) ? 'yes' : 'no') . "\n";
echo 'Is empty string valid? ' . (type_html()->isValid('') ? 'yes' : 'no') . "\n";
