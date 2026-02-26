<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_mixed;

require __DIR__ . '/vendor/autoload.php';

echo 'Assert string: ' . type_mixed()->assert('hello') . "\n";
echo 'Assert int: ' . type_mixed()->assert(42) . "\n";
echo 'Assert null: ' . (type_mixed()->assert(null) === null ? 'null' : 'not null') . "\n";
echo 'Assert array: ' . json_encode(type_mixed()->assert([1, 2, 3])) . "\n";
