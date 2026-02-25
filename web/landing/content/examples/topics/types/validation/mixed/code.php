<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_mixed;

require __DIR__ . '/vendor/autoload.php';

echo 'Is string valid? ' . (type_mixed()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is int valid? ' . (type_mixed()->isValid(42) ? 'yes' : 'no') . "\n";
echo 'Is null valid? ' . (type_mixed()->isValid(null) ? 'yes' : 'no') . "\n";
echo 'Is array valid? ' . (type_mixed()->isValid([1, 2, 3]) ? 'yes' : 'no') . "\n";
echo 'Is object valid? ' . (type_mixed()->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";
