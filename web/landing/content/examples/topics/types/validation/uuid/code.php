<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_uuid;

require __DIR__ . '/vendor/autoload.php';

echo 'Is valid UUID? ' . (type_uuid()->isValid('550e8400-e29b-41d4-a716-446655440000') ? 'yes' : 'no') . "\n";
echo 'Is "not-a-uuid" valid? ' . (type_uuid()->isValid('not-a-uuid') ? 'yes' : 'no') . "\n";
echo 'Is partial UUID valid? ' . (type_uuid()->isValid('550e8400-e29b') ? 'yes' : 'no') . "\n";
