<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_object;

require __DIR__ . '/vendor/autoload.php';

echo 'Is stdClass valid? ' . (type_object()->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";
echo 'Is DateTimeImmutable valid? ' . (type_object()->isValid(new \DateTimeImmutable()) ? 'yes' : 'no') . "\n";
echo 'Is array valid? ' . (type_object()->isValid([1, 2, 3]) ? 'yes' : 'no') . "\n";
echo 'Is string valid? ' . (type_object()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is null valid? ' . (type_object()->isValid(null) ? 'yes' : 'no') . "\n";
