<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry\Type\Uuid;

final class UuidType implements LogicalType
{
    public function __construct(private readonly bool $nullable = false)
    {
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        if (\is_object($value)) {
            foreach ([Uuid::class, \Ramsey\Uuid\UuidInterface::class, \Symfony\Component\Uid\Uuid::class] as $uuidClass) {
                if ($value instanceof $uuidClass) {
                    return true;
                }
            }
        }

        return false;
    }

    public function nullable() : bool
    {
        return $this->nullable;
    }

    public function toString() : string
    {
        return ($this->nullable ? '?' : '') . 'uuid';
    }
}
