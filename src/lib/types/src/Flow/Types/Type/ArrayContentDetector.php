<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{type_array, type_null, type_optional, type_string};
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Native\{IntegerType, NullType, StringType};

final readonly class ArrayContentDetector
{
    /**
     * @var null|Type<array-key>
     */
    private ?Type $firstKeyType;

    /**
     * @var null|Type<mixed>
     */
    private ?Type $firstValueType;

    private int $uniqueKeysTypeCount;

    private int $uniqueValuesTypeCount;

    /**
     * @param Types<array-key> $uniqueKeysType
     * @param Types<mixed> $uniqueValuesType
     * @param bool $isList
     */
    public function __construct(Types $uniqueKeysType, private Types $uniqueValuesType, private bool $isList = false)
    {
        $this->firstKeyType = $uniqueKeysType->first();
        $this->firstValueType = $uniqueValuesType->first();
        $this->uniqueKeysTypeCount = $uniqueKeysType->reduceOptionals()->without(type_array(), type_null())->count();
        $this->uniqueValuesTypeCount = $this->uniqueValuesType->reduceOptionals()->without(type_array(), type_null())->count();
    }

    /**
     * @return null|Type<int|string>
     */
    public function firstKeyType() : ?Type
    {
        if (null !== $this->firstKeyType && (!$this->firstKeyType instanceof IntegerType && !$this->firstKeyType instanceof StringType)) {
            throw new InvalidArgumentException('First unique key type must be of IntegerType or StringType, given: ' . $this->firstKeyType::class);
        }

        return $this->firstKeyType;
    }

    /**
     * @return null|Type<mixed>
     */
    public function firstValueType() : ?Type
    {
        return $this->firstValueType;
    }

    /**
     * @phpstan-assert-if-true Type<int> $this->firstKeyType()
     */
    public function isList() : bool
    {
        return 1 === $this->uniqueValuesTypeCount && $this->firstKeyType() instanceof IntegerType && $this->isList;
    }

    /**
     * @phpstan-assert-if-true Type<int|string> $this->firstKeyType()
     */
    public function isMap() : bool
    {
        return 1 === $this->uniqueValuesTypeCount && 1 === $this->uniqueKeysTypeCount && !$this->isList;
    }

    public function isStructure() : bool
    {
        if ($this->isList() || $this->isMap()) {
            return false;
        }

        return 0 !== $this->uniqueValuesTypeCount
            && 1 === $this->uniqueKeysTypeCount
            && $this->firstKeyType() instanceof StringType;
    }

    /**
     * @return Type<mixed>
     */
    public function valueType() : Type
    {
        $type = null;

        foreach ($this->uniqueValuesType->all() as $nextType) {
            if (null === $type) {
                $type = $nextType;

                continue;
            }

            if ($type instanceof NullType) {
                $type = type_optional($nextType);

                continue;
            }

            if ($nextType instanceof NullType) {
                $type = type_optional($type);
            }
        }

        if ($type === null) {
            return type_optional(type_string());
        }

        return $type;
    }
}
