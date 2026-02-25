<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_list};

require __DIR__ . '/vendor/autoload.php';

echo 'Is [1,2,3] valid? ' . (type_list(type_integer())->isValid([1, 2, 3]) ? 'yes' : 'no') . "\n";
echo 'Is [] valid? ' . (type_list(type_integer())->isValid([]) ? 'yes' : 'no') . "\n";
echo 'Is ["a","b"] valid? ' . (type_list(type_integer())->isValid(['a', 'b']) ? 'yes' : 'no') . "\n";
echo 'Is [1,"two",3] valid? ' . (type_list(type_integer())->isValid([1, 'two', 3]) ? 'yes' : 'no') . "\n";
echo 'Is ["a"=>1] valid? ' . (type_list(type_integer())->isValid(['a' => 1]) ? 'yes' : 'no') . "\n";
