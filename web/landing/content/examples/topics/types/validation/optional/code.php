<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_optional, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

echo 'Is "hello" valid ?string? ' . (type_optional(type_string())->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is null valid ?string? ' . (type_optional(type_string())->isValid(null) ? 'yes' : 'no') . "\n";
echo 'Is 123 valid ?string? ' . (type_optional(type_string())->isValid(123) ? 'yes' : 'no') . "\n";
