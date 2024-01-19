<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Factory\StringTypeChecker;

final class JsonType implements LogicalType
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
        if (!\is_string($value)) {
            return false;
        }

        return (new StringTypeChecker($value))->isJson();
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'json';
    }
}
