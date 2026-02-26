<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_callable;

require __DIR__ . '/vendor/autoload.php';

echo 'Is closure valid? ' . (type_callable()->isValid(fn () => 'test') ? 'yes' : 'no') . "\n";
echo 'Is "strtoupper" valid? ' . (type_callable()->isValid('strtoupper') ? 'yes' : 'no') . "\n";
echo 'Is "nonexistent_func" valid? ' . (type_callable()->isValid('nonexistent_func') ? 'yes' : 'no') . "\n";
echo 'Is string valid? ' . (type_callable()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is array callable valid? ' . (type_callable()->isValid([\DateTime::class, 'createFromFormat']) ? 'yes' : 'no') . "\n";
