<?php

declare(strict_types=1);

use Flow\Types\Exception\CastingException;

use function Flow\Types\DSL\{type_date, type_integer};

require __DIR__ . '/vendor/autoload.php';

// cast() converts, where assert() would have thrown
echo 'integer from string: ' . type_integer()->cast('42') . "\n";
echo 'date from string: ' . type_date()->cast('2024-06-15')->format('Y-m-d') . "\n";

// conversion still has limits
try {
    type_integer()->cast('not a number');
} catch (CastingException $e) {
    echo 'cannot cast: ' . $e->getMessage() . "\n";
}
