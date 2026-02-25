<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

require __DIR__ . '/vendor/autoload.php';

$type = type_map(type_string(), type_integer());

echo 'Cast map: ' . json_encode($type->cast(['a' => '1', 'b' => '2'])) . "\n";
echo 'Cast from array: ' . json_encode($type->cast(['x' => 10, 'y' => 20])) . "\n";
