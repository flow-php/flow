<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_class_string;

require __DIR__ . '/vendor/autoload.php';

echo 'Assert class string: ' . type_class_string()->assert(\DateTimeImmutable::class) . "\n";
echo 'Assert with parent: ' . type_class_string(\DateTimeInterface::class)->assert(\DateTimeImmutable::class) . "\n";
