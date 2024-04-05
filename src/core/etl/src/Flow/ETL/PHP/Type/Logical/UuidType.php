<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type\Logical;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row\Entry\Type\Uuid;

final class UuidType implements LogicalType
{
    public function __construct(private readonly bool $nullable = false)
    {
    }

    public static function fromArray(array $data) : Type
    {
        return new self($data['nullable'] ?? false);
    }

    public function isEqual(Type $type) : bool
    {
        return $type instanceof self;
    }

    public function isValid(mixed $value) : bool
    {
        if ($this->nullable && $value === null) {
            return true;
        }

        if (\is_object($value)) {
            foreach ([Uuid::class, \Ramsey\Uuid\UuidInterface::class, \Symfony\Component\Uid\Uuid::class] as $uuidClass) {
                if ($value instanceof $uuidClass) {
                    return true;
                }
            }
        }

        return false;
    }

    public function makeNullable(bool $nullable) : self
    {
        return new self($nullable);
    }

    public function merge(Type $type) : self
    {
        if ($type instanceof NullType) {
            return $this->makeNullable(true);
        }

        if (!$type instanceof self) {
            throw new InvalidArgumentException('Cannot merge different types, ' . $this->toString() . ' and ' . $type->toString());
        }

        return new self($this->nullable || $type->nullable());
    }

    public function normalize() : array
    {
        return [
            'type' => 'uuid',
            'nullable' => $this->nullable,
        ];
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
