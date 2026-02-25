<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_json;

require __DIR__ . '/vendor/autoload.php';

echo 'From array: ' . type_json()->cast(['id' => 1, 'name' => 'Alice'])->toString() . "\n";
echo 'From string: ' . type_json()->cast('{"product":"Widget"}')->toString() . "\n";
echo 'Access data: ' . type_json()->cast('{"price":29.99}')->data()['price'] . "\n";
