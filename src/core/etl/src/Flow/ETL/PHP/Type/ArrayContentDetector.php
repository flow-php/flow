<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\type_array;
use function Flow\ETL\DSL\type_null;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class ArrayContentDetector
{
    private readonly ?Type $firstKeyType;

    private readonly ?Type $firstValueType;

    private readonly int $uniqueKeysCount;

    private readonly int $uniqueValuesCount;

    public function __construct(Types $uniqueKeysType, Types $uniqueValuesType)
    {
        $this->firstKeyType = $uniqueKeysType->first();
        $this->firstValueType = $uniqueValuesType->first();
        $this->uniqueKeysCount = $uniqueKeysType->count();
        $this->uniqueValuesCount = $uniqueValuesType->without(type_array(true), type_null())->count();
    }

    public function firstKeyType() : ?ScalarType
    {
        if (null !== $this->firstKeyType && !$this->firstKeyType instanceof ScalarType) {
            throw InvalidArgumentException::because('First unique key type must be of ScalarType, given: ' . $this->firstKeyType::class);
        }

        return $this->firstKeyType;
    }

    public function firstValueType() : ?Type
    {
        return $this->firstValueType;
    }

    public function isList() : bool
    {
        return 1 === $this->uniqueValuesCount && $this->firstKeyType()?->isInteger();
    }

    public function isMap() : bool
    {
        if (1 === $this->uniqueValuesCount && 1 === $this->uniqueKeysCount) {
            return !$this->firstKeyType()?->isInteger();
        }

        return false;
    }

    public function isStructure() : bool
    {
        if ($this->isList() || $this->isMap()) {
            return false;
        }

        return 0 !== $this->uniqueValuesCount
            && 1 === $this->uniqueKeysCount
            && $this->firstKeyType()?->isString();
    }
}
