<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_map, type_string};

require __DIR__ . '/vendor/autoload.php';

echo 'Scores: ' . json_encode(type_map(type_string(), type_integer())->assert(['alice' => 100, 'bob' => 85])) . "\n";
