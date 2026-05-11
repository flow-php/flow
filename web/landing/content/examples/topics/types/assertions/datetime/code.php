<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_datetime;

require __DIR__ . '/vendor/autoload.php';

echo 'From DateTimeImmutable: ' . type_datetime()->assert(new \DateTimeImmutable('2024-06-15 14:30:00'))->format('Y-m-d H:i:s') . "\n";
echo 'From DateTime: ' . type_datetime()->assert(new \DateTime('@1704067200'))->format('Y-m-d H:i:s') . "\n";
