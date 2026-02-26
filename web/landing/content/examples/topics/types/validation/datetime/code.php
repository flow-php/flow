<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_datetime;

require __DIR__ . '/vendor/autoload.php';

echo 'Is "2024-01-15" valid? ' . (type_datetime()->isValid('2024-01-15') ? 'yes' : 'no') . "\n";
echo 'Is "invalid-date" valid? ' . (type_datetime()->isValid('invalid-date') ? 'yes' : 'no') . "\n";
echo 'Is timestamp valid? ' . (type_datetime()->isValid(1704067200) ? 'yes' : 'no') . "\n";
echo 'Is DateTimeImmutable valid? ' . (type_datetime()->isValid(new \DateTimeImmutable()) ? 'yes' : 'no') . "\n";
