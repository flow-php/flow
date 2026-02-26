<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_literal;

require __DIR__ . '/vendor/autoload.php';

echo 'Cast literal string: ' . type_literal('active')->cast('active') . "\n";
echo 'Cast literal int: ' . type_literal(42)->cast(42) . "\n";
echo 'Cast literal bool: ' . (type_literal(true)->cast(true) ? 'true' : 'false') . "\n";
