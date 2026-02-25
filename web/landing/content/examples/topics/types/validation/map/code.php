<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_integer, type_map, type_string};

require __DIR__ . '/vendor/autoload.php';

echo 'Is ["a"=>1,"b"=>2] valid? ' . (type_map(type_string(), type_integer())->isValid(['a' => 1, 'b' => 2]) ? 'yes' : 'no') . "\n";
echo 'Is ["a"=>1,"b"=>"x"] valid? ' . (type_map(type_string(), type_integer())->isValid(['a' => 1, 'b' => 'x']) ? 'yes' : 'no') . "\n";
echo 'Is [1=>100,2=>200] valid? ' . (type_map(type_string(), type_integer())->isValid([1 => 100, 2 => 200]) ? 'yes' : 'no') . "\n";
echo 'Is [] valid? ' . (type_map(type_string(), type_integer())->isValid([]) ? 'yes' : 'no') . "\n";
