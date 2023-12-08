<?php declare(strict_types=1);

namespace Flow\RDSL\Tests\Fixtures;

use Flow\RDSL\Attribute\DSL;

#[DSL]
function int(int $value) : IntObject
{
    return (new IntObject())->set($value);
}

#[DSL]
function lit(mixed $value) : Literal
{
    return new Literal($value);
}

#[DSL(exclude: true)]
function exclude() : void
{
}
