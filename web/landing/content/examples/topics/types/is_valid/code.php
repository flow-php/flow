<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_json};

require __DIR__ . '/vendor/autoload.php';

// isValid() answers the same question as assert(), without throwing
foreach ([42, '42', 3.14] as $value) {
    echo var_export($value, true) . ' is integer: ' . (type_integer()->isValid($value) ? 'yes' : 'no') . "\n";
}

// json is a value object: JSON text is only a string until cast() turns it into one
foreach (['text' => '{"name":"John"}', 'cast()' => type_json()->cast('{"name":"John"}')] as $label => $value) {
    echo $label . ' is json: ' . (type_json()->isValid($value) ? 'yes' : 'no') . "\n";
}
