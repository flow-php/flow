<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_array;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast indexed array: ' . json_encode(type_array()->cast([1, 2, 3])) . "\n";
echo 'Cast associative array: ' . json_encode(type_array()->cast(['a' => 1, 'b' => 2])) . "\n";
echo 'Cast empty array: ' . json_encode(type_array()->cast([])) . "\n";
