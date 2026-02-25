<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_non_empty_string, type_numeric_string, type_positive_integer};

require __DIR__ . '/vendor/autoload.php';

echo 'Is 5 positive int? ' . (type_positive_integer()->isValid(5) ? 'yes' : 'no') . "\n";
echo 'Is 0 positive int? ' . (type_positive_integer()->isValid(0) ? 'yes' : 'no') . "\n";
echo 'Is -3 positive int? ' . (type_positive_integer()->isValid(-3) ? 'yes' : 'no') . "\n";
echo 'Is "hello" non-empty? ' . (type_non_empty_string()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is "" non-empty? ' . (type_non_empty_string()->isValid('') ? 'yes' : 'no') . "\n";
echo 'Is "123" numeric string? ' . (type_numeric_string()->isValid('123') ? 'yes' : 'no') . "\n";
echo 'Is "abc" numeric string? ' . (type_numeric_string()->isValid('abc') ? 'yes' : 'no') . "\n";
