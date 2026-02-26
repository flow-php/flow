<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_string, type_union};

require __DIR__ . '/vendor/autoload.php';

echo 'Int: ' . type_union(type_string(), type_integer())->assert(42) . "\n";
echo 'String: ' . type_union(type_string(), type_integer())->assert('ABC-123') . "\n";
