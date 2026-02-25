<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_mixed;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast string: ' . type_mixed()->cast('hello') . "\n";
echo 'Cast int: ' . type_mixed()->cast(42) . "\n";
echo 'Cast null: ' . (type_mixed()->cast(null) === null ? 'null' : 'not null') . "\n";
echo 'Cast array: ' . json_encode(type_mixed()->cast([1, 2, 3])) . "\n";
