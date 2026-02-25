<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_json;

require __DIR__ . '/vendor/autoload.php';

echo 'From string: ' . type_json()->assert('{"user":"john","active":true}')->toString() . "\n";
echo 'From array: ' . type_json()->assert(['product' => 'Widget', 'price' => 29.99])->toString() . "\n";
