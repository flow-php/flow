<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_array;

require __DIR__ . '/vendor/autoload.php';

echo 'Assert indexed array: ' . json_encode(type_array()->assert([1, 2, 3])) . "\n";
echo 'Assert associative array: ' . json_encode(type_array()->assert(['a' => 1, 'b' => 2])) . "\n";
echo 'Assert empty array: ' . json_encode(type_array()->assert([])) . "\n";
