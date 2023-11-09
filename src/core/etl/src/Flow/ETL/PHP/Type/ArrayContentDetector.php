<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\ArrayType;
use Flow\ETL\PHP\Type\Native\NullType;
use Flow\ETL\PHP\Type\Native\ScalarType;

final class ArrayContentDetector
{
    private readonly ?Type $firstKeyType;

    private readonly ?Type $firstValueType;

    public function __construct(private readonly Types $uniqueKeysType, private readonly Types $uniqueValuesType)
    {
        $this->firstKeyType = $this->uniqueKeysType->first();
        $this->firstValueType = $this->uniqueValuesType->first();
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
        if (!$this->firstKeyType()?->isInteger()) {
            return false;
        }

        return 1 === $this->uniqueValuesType->without(new ArrayType(true), new NullType())->count();
    }

    public function isMap() : bool
    {
        if (1 === $this->uniqueValuesType->without(new ArrayType(true), new NullType())->count()) {
            if ($this->isList()) {
                return false;
            }

            if (!($this->firstKeyType()?->isString() || $this->firstKeyType()?->isInteger())) {
                return false;
            }

            return 1 === $this->uniqueKeysType->count();
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
            && 0 !== $this->uniqueValuesType->without(new ArrayType(true), new NullType())->count();
    }
}
