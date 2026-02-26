<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_non_empty_string, type_numeric_string, type_positive_integer};

require __DIR__ . '/vendor/autoload.php';

echo 'Cast "10" to positive int: ' . type_positive_integer()->cast('10') . "\n";
echo 'Cast "hello" to non-empty: ' . type_non_empty_string()->cast('hello') . "\n";
echo 'Cast 123 to numeric string: ' . type_numeric_string()->cast(123) . "\n";
echo 'Cast 45.67 to numeric string: ' . type_numeric_string()->cast(45.67) . "\n";
