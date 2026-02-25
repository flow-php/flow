<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time;

require __DIR__ . '/vendor/autoload.php';

echo 'From time string: ' . type_time()->cast('14:30:00')->format('H:i:s') . "\n";
echo 'From short time: ' . type_time()->cast('09:15')->format('H:i:s') . "\n";
