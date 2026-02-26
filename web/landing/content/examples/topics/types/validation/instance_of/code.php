<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_instance_of;

require __DIR__ . '/vendor/autoload.php';

$date = new \DateTimeImmutable();

echo 'Is DateTimeImmutable a DateTimeInterface? ' . (type_instance_of(\DateTimeInterface::class)->isValid($date) ? 'yes' : 'no') . "\n";
echo 'Is stdClass a DateTimeInterface? ' . (type_instance_of(\DateTimeInterface::class)->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";
echo 'Is string a DateTimeInterface? ' . (type_instance_of(\DateTimeInterface::class)->isValid('2024-01-15') ? 'yes' : 'no') . "\n";
