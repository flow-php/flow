<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_json;
use Flow\Types\Value\Json;

require __DIR__ . '/vendor/autoload.php';

echo 'From Json string: ' . type_json()->assert(Json::fromString('{"user":"john","active":true}'))->toString() . "\n";
echo 'From Json array: ' . type_json()->assert(Json::fromArray(['product' => 'Widget', 'price' => 29.99]))->toString() . "\n";
