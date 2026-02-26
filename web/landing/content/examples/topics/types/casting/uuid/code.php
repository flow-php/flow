<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_uuid;

require __DIR__ . '/vendor/autoload.php';

echo 'Lowercase: ' . type_uuid()->cast('550e8400-e29b-41d4-a716-446655440000')->toString() . "\n";
echo 'Uppercase: ' . type_uuid()->cast('6BA7B810-9DAD-11D1-80B4-00C04FD430C8')->toString() . "\n";
