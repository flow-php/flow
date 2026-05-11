<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time;

require __DIR__ . '/vendor/autoload.php';

echo 'From time string: ' . type_time()->cast('PT14H30M0S')->format('%H:%I:%S') . "\n";
echo 'From short time: ' . type_time()->cast('PT9H15M0S')->format('%H:%I:%S') . "\n";
