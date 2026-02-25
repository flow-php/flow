<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_instance_of;

require __DIR__ . '/vendor/autoload.php';

$date = new \DateTimeImmutable('2024-01-15');

echo 'Assert instance of: ' . type_instance_of(\DateTimeInterface::class)->assert($date)->format('Y-m-d') . "\n";
echo 'Assert exact class: ' . type_instance_of(\DateTimeImmutable::class)->assert($date)->format('Y-m-d') . "\n";
