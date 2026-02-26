<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_datetime;

require __DIR__ . '/vendor/autoload.php';

echo 'From string: ' . type_datetime()->cast('2024-06-15')->format('Y-m-d') . "\n";
echo 'From datetime string: ' . type_datetime()->cast('2024-12-25 10:30:00')->format('Y-m-d H:i:s') . "\n";
echo 'From timestamp: ' . type_datetime()->cast(1704067200)->format('Y-m-d H:i:s') . "\n";
