<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_literal;

require __DIR__ . '/vendor/autoload.php';

echo 'Assert literal string: ' . type_literal('active')->assert('active') . "\n";
echo 'Assert literal int: ' . type_literal(42)->assert(42) . "\n";
echo 'Assert literal bool: ' . (type_literal(true)->assert(true) ? 'true' : 'false') . "\n";
