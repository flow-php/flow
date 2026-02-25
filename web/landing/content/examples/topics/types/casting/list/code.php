<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_list, type_string};

require __DIR__ . '/vendor/autoload.php';

echo 'Strings to ints: ' . json_encode(type_list(type_integer())->cast(['1', '2', '3'])) . "\n";
echo 'Ints to strings: ' . json_encode(type_list(type_string())->cast([10, 20, 30])) . "\n";
