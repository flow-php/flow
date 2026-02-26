<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_class_string;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast class string: ' . type_class_string()->cast(\DateTimeImmutable::class) . "\n";
echo 'Cast with parent: ' . type_class_string(\DateTimeInterface::class)->cast(\DateTimeImmutable::class) . "\n";
