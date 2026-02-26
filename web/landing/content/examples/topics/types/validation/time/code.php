<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_time;

require __DIR__ . '/vendor/autoload.php';

echo 'Is "14:30:00" valid? ' . (type_time()->isValid('14:30:00') ? 'yes' : 'no') . "\n";
echo 'Is "09:15" valid? ' . (type_time()->isValid('09:15') ? 'yes' : 'no') . "\n";
echo 'Is "25:00:00" valid? ' . (type_time()->isValid('25:00:00') ? 'yes' : 'no') . "\n";
echo 'Is "invalid" valid? ' . (type_time()->isValid('invalid') ? 'yes' : 'no') . "\n";
