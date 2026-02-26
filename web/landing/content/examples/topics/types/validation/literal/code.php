<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_literal;

require __DIR__ . '/vendor/autoload.php';

echo 'Is "active" valid for literal("active")? ' . (type_literal('active')->isValid('active') ? 'yes' : 'no') . "\n";
echo 'Is "inactive" valid for literal("active")? ' . (type_literal('active')->isValid('inactive') ? 'yes' : 'no') . "\n";
echo 'Is 42 valid for literal(42)? ' . (type_literal(42)->isValid(42) ? 'yes' : 'no') . "\n";
echo 'Is 43 valid for literal(42)? ' . (type_literal(42)->isValid(43) ? 'yes' : 'no') . "\n";
echo 'Is true valid for literal(true)? ' . (type_literal(true)->isValid(true) ? 'yes' : 'no') . "\n";
