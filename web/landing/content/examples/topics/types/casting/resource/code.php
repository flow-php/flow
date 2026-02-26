<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_resource;

require __DIR__ . '/vendor/autoload.php';

$handle = fopen('php://memory', 'r+');
$result = type_resource()->cast($handle);

echo 'Cast resource: ' . get_resource_type($result) . "\n";
echo 'Is resource: ' . (is_resource($result) ? 'yes' : 'no') . "\n";

fclose($handle);
