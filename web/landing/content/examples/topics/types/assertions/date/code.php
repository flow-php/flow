<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_date;

require __DIR__ . '/vendor/autoload.php';

echo 'From DateTimeImmutable: ' . type_date()->assert(new \DateTimeImmutable('2024-06-15'))->format('Y-m-d') . "\n";
echo 'From DateTime: ' . type_date()->assert((new \DateTime('2024-12-25'))->setTime(0, 0, 0))->format('Y-m-d') . "\n";
