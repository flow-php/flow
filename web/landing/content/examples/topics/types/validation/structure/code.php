<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_boolean, type_integer, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

$userType = type_structure(['id' => type_integer(), 'name' => type_string(), 'active' => type_boolean()]);

echo 'Valid user? ' . ($userType->isValid(['id' => 1, 'name' => 'John', 'active' => true]) ? 'yes' : 'no') . "\n";
echo 'Missing field? ' . ($userType->isValid(['id' => 1, 'name' => 'John']) ? 'yes' : 'no') . "\n";
echo 'Wrong type? ' . ($userType->isValid(['id' => 'one', 'name' => 'John', 'active' => true]) ? 'yes' : 'no') . "\n";
echo 'Extra field? ' . ($userType->isValid(['id' => 1, 'name' => 'John', 'active' => true, 'email' => 'j@x.com']) ? 'yes' : 'no') . "\n";
