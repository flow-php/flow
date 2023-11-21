<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class ArrayContentDetector
{
    private readonly int $countUniqueValuesWithoutEmptyData;

    public function __construct(private readonly Types $uniqueKeysType, private readonly Types $uniqueValuesType)
    {
        $this->countUniqueValuesWithoutEmptyData = $this->uniqueValuesType->without(ArrayType::empty(), new NullType())->count();
    }

    public function firstKeyType() : ?ScalarType
    {
        $type = $this->uniqueKeysType->first();

        if (null !== $type && !$type instanceof ScalarType) {
            throw InvalidArgumentException::because('First unique key type must be of ScalarType, given: ' . $type::class);
        }

        return $type;
    }

    public function firstValueType() : ?Type
    {
        return $this->uniqueValuesType->first();
    }

    public function isList() : bool
    {
        if (!$this->firstKeyType()?->isInteger()) {
            return false;
        }

        return 1 === $this->countUniqueValuesWithoutEmptyData;
    }

    public function isMap() : bool
    {
        if (!$this->firstKeyType()?->isValidArrayKey()) {
            return false;
        }

        if (1 === $this->countUniqueValuesWithoutEmptyData) {
            /** @psalm-suppress PossiblyNullReference */
            if (!$this->firstKeyType()->isInteger()) {
                return 1 === $this->uniqueKeysType->count();
            }
        }

        return false;
    }

    public function isStructure() : bool
    {
        if ($this->isList() || $this->isMap()) {
            return false;
        }

        return $this->firstKeyType()?->isString()
            && 1 === $this->uniqueKeysType->count()
            && 0 !== $this->countUniqueValuesWithoutEmptyData;
    }
}
