<?php

declare(strict_types=1);

use Flow\Types\Exception\InvalidArgumentException;

use function Flow\Types\DSL\{type_integer, type_string};

require __DIR__ . '/vendor/autoload.php';

// assert() returns the value narrowed to the asserted type, so static analysis follows it
$id = type_integer()->assert(42);
$name = type_string()->assert('Norbert');

echo "id: {$id}, name: {$name}\n";

// a value of the wrong type throws instead of being coerced
try {
    type_integer()->assert('42');
} catch (InvalidArgumentException $e) {
    echo 'string "42" is not an integer: ' . $e->getMessage() . "\n";
}
