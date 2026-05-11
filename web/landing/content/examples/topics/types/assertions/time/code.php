<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time;

require __DIR__ . '/vendor/autoload.php';

echo 'From DateInterval: ' . type_time()->assert(new \DateInterval('PT14H30M0S'))->format('%H:%I:%S') . "\n";
echo 'From short interval: ' . type_time()->assert(new \DateInterval('PT9H15M0S'))->format('%H:%I:%S') . "\n";
