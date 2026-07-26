<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_uuid;
use Flow\Types\Value\Uuid;

require __DIR__ . '/vendor/autoload.php';

echo 'UUID: ' . type_uuid()->assert(new Uuid('550e8400-e29b-41d4-a716-446655440000'))->toString() . "\n";
echo 'From string: ' . type_uuid()->assert(Uuid::fromString('6ba7b810-9dad-11d1-80b4-00c04fd430c8'))->toString() . "\n";
