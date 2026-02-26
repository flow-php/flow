<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_boolean;

require __DIR__ . '/vendor/autoload.php';

echo 'Is true valid? ' . (type_boolean()->isValid(true) ? 'yes' : 'no') . "\n";
echo 'Is false valid? ' . (type_boolean()->isValid(false) ? 'yes' : 'no') . "\n";
echo 'Is "yes" valid? ' . (type_boolean()->isValid('yes') ? 'yes' : 'no') . "\n";
echo 'Is 1 valid? ' . (type_boolean()->isValid(1) ? 'yes' : 'no') . "\n";
echo 'Is "true" valid? ' . (type_boolean()->isValid('true') ? 'yes' : 'no') . "\n";
