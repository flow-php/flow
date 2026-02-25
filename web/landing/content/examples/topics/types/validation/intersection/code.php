<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_intersection;

require __DIR__ . '/vendor/autoload.php';

$type = type_intersection(
    type_instance_of(\Countable::class),
    type_instance_of(\IteratorAggregate::class)
);

echo 'Is ArrayObject valid? ' . ($type->isValid(new \ArrayObject([1, 2, 3])) ? 'yes' : 'no') . "\n";
echo 'Is simple array valid? ' . ($type->isValid([1, 2, 3]) ? 'yes' : 'no') . "\n";
echo 'Is stdClass valid? ' . ($type->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";
