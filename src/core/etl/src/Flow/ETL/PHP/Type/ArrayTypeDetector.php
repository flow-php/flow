<?php
declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class ArrayTypeDetector
{
    private readonly array $uniqueKeysType;

    private readonly array $uniqueValuesType;

    public function __construct(
        array $keysTypes,
        array $valuesTypes,
    ) {
        if (0 === \count($keysTypes)) {
            throw new InvalidArgumentException('Key type list cannot be empty');
        }

        if (0 === \count($valuesTypes)) {
            throw new InvalidArgumentException('Value type list cannot be empty');
        }

        $this->uniqueKeysType = \array_unique(\array_map(fn (Type $type) : string => \serialize($type), $keysTypes));
        $this->uniqueValuesType = \array_unique(
            \array_map(fn (Type $type) : string => \serialize($type), \array_filter($valuesTypes, function (Type $type) : bool {
                if ($type instanceof NullType) {
                    return false;
                }

                return !($type instanceof ArrayType && $type->empty());
            }))
        );
    }

    public function firstKeyType() : ScalarType
    {
        return \unserialize($this->uniqueKeysType[0]);
    }

    public function firstValueType() : Type
    {
        return \unserialize($this->uniqueValuesType[0]);
    }

    public function isList() : bool
    {
        return 1 === \count($this->uniqueValuesType) && $this->firstKeyType()->isInteger();
    }

    public function isMap() : bool
    {
        if (1 === \count($this->uniqueValuesType)) {
            if ($this->isList()) {
                return false;
            }

            $type = $this->firstKeyType();

            if (1 === \count($this->uniqueKeysType) && ($type->isString() || $type->isInteger())) {
                return true;
            }
        }

        return false;
    }

    public function isStructure() : bool
    {
        if ($this->isList() || $this->isMap()) {
            return false;
        }

        return 1 === \count($this->uniqueKeysType) && $this->firstKeyType()->isString();
    }
}
