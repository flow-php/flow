<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_object;

require __DIR__ . '/vendor/autoload.php';

$obj = new \stdClass();
$obj->name = 'test';

echo 'Assert object: ' . get_class(type_object()->assert($obj)) . "\n";
echo 'Property: ' . type_object()->assert($obj)->name . "\n";
echo 'Assert datetime: ' . get_class(type_object()->assert(new \DateTimeImmutable())) . "\n";
