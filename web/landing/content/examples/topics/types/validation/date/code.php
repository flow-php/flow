<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_date;

require __DIR__ . '/vendor/autoload.php';

echo 'Is "2024-01-15" valid? ' . (type_date()->isValid('2024-01-15') ? 'yes' : 'no') . "\n";
echo 'Is "2024-12-25 10:30" valid? ' . (type_date()->isValid('2024-12-25 10:30') ? 'yes' : 'no') . "\n";
echo 'Is "invalid-date" valid? ' . (type_date()->isValid('invalid-date') ? 'yes' : 'no') . "\n";
echo 'Is DateTimeImmutable valid? ' . (type_date()->isValid(new \DateTimeImmutable()) ? 'yes' : 'no') . "\n";
