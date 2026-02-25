<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_list, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

echo 'Int list: ' . json_encode(type_list(type_integer())->assert([85, 90, 78, 92])) . "\n";
echo 'String list: ' . json_encode(type_list(type_string())->assert(['Alice', 'Bob', 'Charlie'])) . "\n";
echo 'Structure list: ' . json_encode(type_list(type_structure(['id' => type_integer(), 'name' => type_string()]))->assert([['id' => 1, 'name' => 'John'], ['id' => 2, 'name' => 'Jane']])) . "\n";
