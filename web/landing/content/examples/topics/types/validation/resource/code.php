<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_resource;

require __DIR__ . '/vendor/autoload.php';

$handle = fopen('php://memory', 'r+');

echo 'Is file handle valid? ' . (type_resource()->isValid($handle) ? 'yes' : 'no') . "\n";
echo 'Is string valid? ' . (type_resource()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is object valid? ' . (type_resource()->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";

fclose($handle);
echo 'Is closed handle valid? ' . (type_resource()->isValid($handle) ? 'yes' : 'no') . "\n";
