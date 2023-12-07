<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Fixtures;

function int(int $value) : IntObject
{
    return (new IntObject())->set($value);
}

function lit(mixed $value) : Literal
{
    return new Literal($value);
}
