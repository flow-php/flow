<?php

declare(strict_types=1);

namespace Flow\CLI;

use Flow\CLI\Arguments\TypedArgument;
use Flow\CLI\Options\{TypedOption};
use Symfony\Component\Console\Input\InputInterface;

function option_bool(string $name, InputInterface $input) : bool
{
    return (new TypedOption($name))->asBool($input);
}

function option_bool_nullable(string $name, InputInterface $input) : ?bool
{
    return (new TypedOption($name))->asBoolNullable($input);
}

function option_string(string $name, InputInterface $input, ?string $default = null) : string
{
    return (new TypedOption($name))->asString($input, $default);
}

function option_string_nullable(string $name, InputInterface $input) : ?string
{
    return (new TypedOption($name))->asStringNullable($input);
}

function option_int(string $name, InputInterface $input, ?int $default = null) : int
{
    return (new TypedOption($name))->asInt($input, $default);
}

function option_int_nullable(string $name, InputInterface $input) : ?int
{
    return (new TypedOption($name))->asIntNullable($input);
}

function option_list_of_strings(string $name, InputInterface $input) : array
{
    return (new TypedOption($name))->asListOfStrings($input);
}

function option_list_of_strings_nullable(string $name, InputInterface $input) : ?array
{
    return (new TypedOption($name))->asListOfStringsNullable($input);
}

function argument_string(string $name, InputInterface $input) : string
{
    return (new TypedArgument($name))->asString($input);
}
