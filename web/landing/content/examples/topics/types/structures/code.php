<?php

declare(strict_types=1);

use function Flow\Types\DSL\{type_boolean, type_integer, type_list, type_string, type_structure};

require __DIR__ . '/vendor/autoload.php';

if (\file_exists(__DIR__ . '/output.txt')) {
    unlink(__DIR__ . '/output.txt');
}

$userType = type_structure([
    'id' => type_integer(),
    'name' => type_string(),
    'active' => type_boolean(),
]);

$userListType = type_list($userType);

$data = [
    ['id' => 1, 'name' => 'John', 'active' => true],
    ['id' => 2, 'name' => 'Rick', 'active' => true],
    ['id' => 3, 'name' => 'Albert', 'active' => false],
];

/**
 * @param array{id:int,name:string,active:bool} $userData
 */
function printUser(array $userData) : void
{
    file_put_contents(__DIR__ . '/output.txt', \json_encode($userData) . "\n", FILE_APPEND);
}

if ($userListType->isValid($data)) {
    foreach ($data as $userData) {
        // at this point static analysis knows the shape of each element of $data array
        printUser($userData);
    }
}
