<?php

declare(strict_types=1);

namespace Flow\ETL\PHP\Type;

use function Flow\ETL\DSL\{type_array, type_null, type_string};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PHP\Type\Native\{IntegerType, NullType, StringType};

final readonly class ArrayContentDetector
{
    /**
     * @var null|Type<mixed>
     */
    private ?Type $firstKeyType;

    /**
     * @var null|Type<mixed>
     */
    private ?Type $firstValueType;

    private int $uniqueKeysTypeCount;

    private int $uniqueValuesTypeCount;

    public function __construct(Types $uniqueKeysType, private Types $uniqueValuesType, private bool $isList = false)
    {
        $this->firstKeyType = $uniqueKeysType->first();
        $this->firstValueType = $uniqueValuesType->first();
        $this->uniqueKeysTypeCount = $uniqueKeysType->count();
        $this->uniqueValuesTypeCount = $uniqueValuesType->without(type_array(true), type_null())->count();
    }

    public function firstKeyType() : IntegerType|StringType|null
    {
        if (null !== $this->firstKeyType && (!$this->firstKeyType instanceof IntegerType && !$this->firstKeyType instanceof StringType)) {
            throw InvalidArgumentException::because('First unique key type must be of IntegerType or StringType, given: ' . $this->firstKeyType::class);
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

    public function isList() : bool
    {
        return 1 === $this->uniqueValuesTypeCount && $this->firstKeyType() instanceof IntegerType && $this->isList;
    }

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
                $type = $nextType->makeNullable(true);

                continue;
            }

            if ($nextType instanceof NullType) {
                $type = $type->makeNullable(true);
            }
        }

        if ($type === null) {
            return type_string(true);
        }

        return $type;
    }
}
