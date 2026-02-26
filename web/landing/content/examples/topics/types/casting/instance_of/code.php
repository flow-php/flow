<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_instance_of;

require __DIR__ . '/vendor/autoload.php';

$date = new \DateTimeImmutable('2024-01-15');

echo 'Cast instance of: ' . type_instance_of(\DateTimeInterface::class)->cast($date)->format('Y-m-d') . "\n";
echo 'Cast exact class: ' . type_instance_of(\DateTimeImmutable::class)->cast($date)->format('Y-m-d') . "\n";
