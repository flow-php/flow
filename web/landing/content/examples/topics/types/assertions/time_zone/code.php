<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time_zone;

require __DIR__ . '/vendor/autoload.php';

echo 'From DateTimeZone: ' . type_time_zone()->assert(new \DateTimeZone('Europe/Warsaw'))->getName() . "\n";
echo 'From UTC: ' . type_time_zone()->assert(new \DateTimeZone('UTC'))->getName() . "\n";
