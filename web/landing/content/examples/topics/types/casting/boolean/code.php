<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_boolean;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast "yes": ' . (type_boolean()->cast('yes') ? 'true' : 'false') . "\n";
echo 'Cast "no": ' . (type_boolean()->cast('no') ? 'true' : 'false') . "\n";
echo 'Cast "1": ' . (type_boolean()->cast('1') ? 'true' : 'false') . "\n";
echo 'Cast "0": ' . (type_boolean()->cast('0') ? 'true' : 'false') . "\n";
echo 'Cast 1: ' . (type_boolean()->cast(1) ? 'true' : 'false') . "\n";
echo 'Cast 0: ' . (type_boolean()->cast(0) ? 'true' : 'false') . "\n";
