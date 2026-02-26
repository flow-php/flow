<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_class_string;

require __DIR__ . '/vendor/autoload.php';

echo 'Is DateTimeImmutable::class valid? ' . (type_class_string()->isValid(\DateTimeImmutable::class) ? 'yes' : 'no') . "\n";
echo 'Is "NonExistentClass" valid? ' . (type_class_string()->isValid('NonExistentClass') ? 'yes' : 'no') . "\n";
echo 'Is DateTimeImmutable valid for DateTimeInterface? ' . (type_class_string(\DateTimeInterface::class)->isValid(\DateTimeImmutable::class) ? 'yes' : 'no') . "\n";
echo 'Is stdClass valid for DateTimeInterface? ' . (type_class_string(\DateTimeInterface::class)->isValid(\stdClass::class) ? 'yes' : 'no') . "\n";
