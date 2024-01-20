<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Type;

final class XMLType implements LogicalType
{
    public function __construct(private readonly bool $nullable)
    {
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        if ($value instanceof \DOMDocument) {
            return true;
        }

        return false;
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'xml';
    }

    public function makeNullable(bool $nullable): Type
    {
        return new self($nullable);
    }
}
