<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time_zone;

require __DIR__ . '/vendor/autoload.php';

echo 'Is "Europe/Warsaw" valid? ' . (type_time_zone()->isValid('Europe/Warsaw') ? 'yes' : 'no') . "\n";
echo 'Is "UTC" valid? ' . (type_time_zone()->isValid('UTC') ? 'yes' : 'no') . "\n";
echo 'Is "America/New_York" valid? ' . (type_time_zone()->isValid('America/New_York') ? 'yes' : 'no') . "\n";
echo 'Is "Invalid/Zone" valid? ' . (type_time_zone()->isValid('Invalid/Zone') ? 'yes' : 'no') . "\n";
echo 'Is DateTimeZone valid? ' . (type_time_zone()->isValid(new \DateTimeZone('UTC')) ? 'yes' : 'no') . "\n";
