<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_scalar;

require __DIR__ . '/vendor/autoload.php';

echo 'Is 42 valid? ' . (type_scalar()->isValid(42) ? 'yes' : 'no') . "\n";
echo 'Is 3.14 valid? ' . (type_scalar()->isValid(3.14) ? 'yes' : 'no') . "\n";
echo 'Is "hello" valid? ' . (type_scalar()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is true valid? ' . (type_scalar()->isValid(true) ? 'yes' : 'no') . "\n";
echo 'Is array valid? ' . (type_scalar()->isValid([1, 2]) ? 'yes' : 'no') . "\n";
echo 'Is null valid? ' . (type_scalar()->isValid(null) ? 'yes' : 'no') . "\n";
