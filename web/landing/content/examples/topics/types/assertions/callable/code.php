<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_callable;

require __DIR__ . '/vendor/autoload.php';

$callback = fn (int $x) => $x * 2;
$result = type_callable()->assert($callback);

echo 'Assert closure: ' . (is_callable($result) ? 'callable' : 'not callable') . "\n";
echo 'Call result: ' . $result(5) . "\n";
echo 'Assert built-in: ' . (is_callable(type_callable()->assert('strtoupper')) ? 'callable' : 'not callable') . "\n";
