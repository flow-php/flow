<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_null, type_string, type_union};

require __DIR__ . '/vendor/autoload.php';

echo 'Is "hello" valid string|int? ' . (type_union(type_string(), type_integer())->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is 42 valid string|int? ' . (type_union(type_string(), type_integer())->isValid(42) ? 'yes' : 'no') . "\n";
echo 'Is 3.14 valid string|int? ' . (type_union(type_string(), type_integer())->isValid(3.14) ? 'yes' : 'no') . "\n";
echo 'Is null valid string|null? ' . (type_union(type_string(), type_null())->isValid(null) ? 'yes' : 'no') . "\n";
