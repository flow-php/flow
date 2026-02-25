<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time_zone;

require __DIR__ . '/vendor/autoload.php';

echo 'From string: ' . type_time_zone()->assert('Europe/Warsaw')->getName() . "\n";
echo 'From UTC: ' . type_time_zone()->assert('UTC')->getName() . "\n";
