<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_boolean, type_integer, type_structure};

require __DIR__ . '/vendor/autoload.php';

$result = type_structure(['id' => type_integer(), 'score' => type_integer(), 'active' => type_boolean()])->cast(['id' => '42', 'score' => '95', 'active' => '1']);

echo 'Cast result: ' . json_encode($result) . "\n";
echo 'ID type: ' . gettype($result['id']) . "\n";
echo 'Active type: ' . gettype($result['active']) . "\n";
