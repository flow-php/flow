<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_json;

require __DIR__ . '/vendor/autoload.php';

echo 'Is array valid? ' . (type_json()->isValid(['name' => 'John']) ? 'yes' : 'no') . "\n";
echo 'Is JSON string valid? ' . (type_json()->isValid('{"name":"John"}') ? 'yes' : 'no') . "\n";
echo 'Is plain string valid? ' . (type_json()->isValid('hello') ? 'yes' : 'no') . "\n";
echo 'Is invalid JSON valid? ' . (type_json()->isValid('{invalid}') ? 'yes' : 'no') . "\n";
