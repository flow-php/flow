<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

require __DIR__ . '/vendor/autoload.php';

$type = type_union(type_string(), type_integer());

echo 'Cast string: ' . $type->cast('hello') . "\n";
echo 'Cast integer: ' . $type->cast(42) . "\n";
echo 'Cast numeric string: ' . $type->cast('123') . "\n";
