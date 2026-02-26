<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_callable;

require __DIR__ . '/vendor/autoload.php';

$callback = fn (int $x) => $x * 2;
$result = type_callable()->cast($callback);

echo 'Cast closure: ' . (is_callable($result) ? 'callable' : 'not callable') . "\n";
echo 'Call result: ' . $result(5) . "\n";
echo 'Cast built-in: ' . (is_callable(type_callable()->cast('strtoupper')) ? 'callable' : 'not callable') . "\n";
