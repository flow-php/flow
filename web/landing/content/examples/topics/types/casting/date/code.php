<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_date;

require __DIR__ . '/vendor/autoload.php';

echo 'From string: ' . type_date()->cast('2024-06-15')->format('Y-m-d') . "\n";
echo 'From datetime string: ' . type_date()->cast('2024-12-25 10:30:00')->format('Y-m-d') . "\n";
