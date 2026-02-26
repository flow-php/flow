<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_non_empty_string, type_positive_integer};

require __DIR__ . '/vendor/autoload.php';

echo 'Positive int: ' . type_positive_integer()->assert(5) . "\n";
echo 'Non-empty string: ' . type_non_empty_string()->assert('Widget Pro') . "\n";
