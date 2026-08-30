<?php

declare(strict_types=1);

use function Flow\Types\DSL\{structure_element, type_boolean, type_integer, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

echo 'User: ' . json_encode(type_structure(['id' => type_integer(), 'name' => type_string(), 'active' => type_boolean()])->assert(['id' => 1, 'name' => 'John Doe', 'active' => true])) . "\n";
echo 'With optional: ' . json_encode(type_structure(['id' => type_integer(), 'name' => type_string(), 'description' => structure_element('description', type_string(), optional: true)])->assert(['id' => 100, 'name' => 'Widget'])) . "\n";
