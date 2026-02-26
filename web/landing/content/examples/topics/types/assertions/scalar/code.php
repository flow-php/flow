<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_scalar;

require __DIR__ . '/vendor/autoload.php';

echo 'Assert integer: ' . type_scalar()->assert(42) . "\n";
echo 'Assert float: ' . type_scalar()->assert(3.14) . "\n";
echo 'Assert string: ' . type_scalar()->assert('hello') . "\n";
echo 'Assert boolean: ' . (type_scalar()->assert(true) ? 'true' : 'false') . "\n";
