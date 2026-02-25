<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_scalar;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast integer: ' . type_scalar()->cast(42) . "\n";
echo 'Cast float: ' . type_scalar()->cast(3.14) . "\n";
echo 'Cast string: ' . type_scalar()->cast('hello') . "\n";
echo 'Cast boolean: ' . (type_scalar()->cast(true) ? 'true' : 'false') . "\n";
