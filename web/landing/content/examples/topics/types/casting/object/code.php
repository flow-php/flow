<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_object;

require __DIR__ . '/vendor/autoload.php';

$obj = new \stdClass();
$obj->name = 'test';

echo 'Cast object: ' . get_class(type_object()->cast($obj)) . "\n";
echo 'Property: ' . type_object()->cast($obj)->name . "\n";
echo 'Cast datetime: ' . get_class(type_object()->cast(new \DateTimeImmutable())) . "\n";
