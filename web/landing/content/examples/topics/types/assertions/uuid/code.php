<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_uuid;

require __DIR__ . '/vendor/autoload.php';

echo 'UUID: ' . type_uuid()->assert('550e8400-e29b-41d4-a716-446655440000')->toString() . "\n";
echo 'Uppercase: ' . type_uuid()->assert('550E8400-E29B-41D4-A716-446655440000')->toString() . "\n";
