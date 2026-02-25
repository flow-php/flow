<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_optional;

require __DIR__ . '/vendor/autoload.php';

$type = type_optional(type_integer());

echo 'Cast value: ' . ($type->cast('42') ?? 'null') . "\n";
echo 'Cast null: ' . ($type->cast(null) ?? 'null') . "\n";
echo 'Cast integer: ' . ($type->cast(100) ?? 'null') . "\n";
