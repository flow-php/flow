<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_optional, type_string};

require __DIR__ . '/vendor/autoload.php';

echo 'Null: ' . (type_optional(type_string())->assert(null) ?? 'No value') . "\n";
echo 'String: ' . (type_optional(type_string())->assert('CoolUser123') ?? 'No value') . "\n";
