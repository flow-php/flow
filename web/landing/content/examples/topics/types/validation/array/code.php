<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_array;

require __DIR__ . '/vendor/autoload.php';

echo 'Is indexed array valid? ' . (type_array()->isValid([1, 2, 3]) ? 'yes' : 'no') . "\n";
echo 'Is associative array valid? ' . (type_array()->isValid(['a' => 1, 'b' => 2]) ? 'yes' : 'no') . "\n";
echo 'Is empty array valid? ' . (type_array()->isValid([]) ? 'yes' : 'no') . "\n";
echo 'Is string valid? ' . (type_array()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is object valid? ' . (type_array()->isValid(new \stdClass()) ? 'yes' : 'no') . "\n";
