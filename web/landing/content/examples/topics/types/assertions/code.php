<?php

declare(strict_types=1);

use function Flow\Types\DSL\type_string;

require __DIR__ . '/vendor/autoload.php';

function userInput() : mixed
{
    return 'this-is-user-input';
}

function generateOutput(string $string) : void
{
}

$input = userInput();
$string = type_string()->assert($input);
// at this point static analysis tools knows that $string is type string
generateOutput($string);
