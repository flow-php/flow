<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_json};

require __DIR__ . '/vendor/autoload.php';

// isValid() answers the same question as assert(), without throwing
foreach ([42, '42', 3.14] as $value) {
    echo var_export($value, true) . ' is integer: ' . (type_integer()->isValid($value) ? 'yes' : 'no') . "\n";
}

foreach (['{"name":"John"}', 'hello', ['name' => 'John']] as $value) {
    echo var_export($value, true) . ' is json: ' . (type_json()->isValid($value) ? 'yes' : 'no') . "\n";
}
