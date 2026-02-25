<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_intersection;

require __DIR__ . '/vendor/autoload.php';

$type = type_intersection(
    type_instance_of(\Countable::class),
    type_instance_of(\IteratorAggregate::class)
);

$collection = new \ArrayObject([1, 2, 3]);

echo 'Assert intersection: ' . get_class($type->assert($collection)) . "\n";
echo 'Count: ' . count($type->assert($collection)) . "\n";
