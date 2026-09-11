<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

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

    /**
     * @var null|Type<mixed>
     */
    private ?Type $unifiedValueType;

    private bool $valueTypesConsistent;

    /**
     * @param Types<mixed> $uniqueKeysType
     * @param Types<mixed> $uniqueValuesType
     * @param bool $isList
     */
    public function __construct(
        Types $uniqueKeysType,
        private Types $uniqueValuesType,
        private bool $isList = false,
        private TypeWidener $unifier = new TypeWidener(),
    ) {
        $this->firstKeyType = $uniqueKeysType->first();
        $this->firstValueType = $uniqueValuesType->first();
        $this->uniqueKeysTypeCount = $uniqueKeysType
            ->reduceOptionals()
            ->without(type_array(), type_empty_array(), type_null())
            ->count();

        $reducedValueTypes = $this->uniqueValuesType->reduceOptionals();
        $countedValueTypes = $reducedValueTypes->without(type_array(), type_empty_array(), type_null());

        $unified = null;

        foreach ($countedValueTypes->all() as $valueType) {
            $unified = $unified === null ? $valueType : $this->unifier->widen($unified, $valueType);
        }

        $this->unifiedValueType = $unified;
        // Ignoring array<mixed>/array{} values in the unification is only sound when the unified
        // type is an array itself - a scalar mixed with arrays has no common list/map value type.
        $this->valueTypesConsistent =
            !$reducedValueTypes->hasAny(type_array(), type_empty_array())
            || $countedValueTypes->first() instanceof ListType
            || $countedValueTypes->first() instanceof MapType
            || $countedValueTypes->first() instanceof StructureType;
    }

    /**
     * @return null|Type<int|string>
     */
    public function firstKeyType(): ?Type
    {
        if (
            null !== $this->firstKeyType
            && (!$this->firstKeyType instanceof IntegerType && !$this->firstKeyType instanceof StringType)
        ) {
            throw new InvalidArgumentException('First unique key type must be of IntegerType or StringType, given: '
            . $this->firstKeyType::class);
        }

        return $this->firstKeyType;
    }

    /**
     * @return null|Type<mixed>
     */
    public function firstValueType(): ?Type
    {
        return $this->firstValueType;
    }

    /**
     * @assert-if-true Type<int> $this->firstKeyType()
     */
    public function isList(): bool
    {
        return (
            null !== $this->unifiedValueType
            && $this->valueTypesConsistent
            && $this->firstKeyType() instanceof IntegerType
            && $this->isList
        );
    }

    /**
     * @assert-if-true Type<int|string> $this->firstKeyType()
     */
    public function isMap(): bool
    {
        if ($this->firstKeyType() instanceof StringType) {
            return false;
        }

        return (
            null !== $this->unifiedValueType
            && 1 === $this->uniqueKeysTypeCount
            && $this->valueTypesConsistent
            && !$this->isList
        );
    }

    public function isStructure(): bool
    {
        if ($this->isList() || $this->isMap()) {
            return false;
        }

        return 1 === $this->uniqueKeysTypeCount && $this->firstKeyType() instanceof StringType;
    }

    /**
     * @return Type<mixed>
     */
    public function valueType(): Type
    {
        $type = $this->unifiedValueType;
        $nullable = false;
        $hasUntypedArray = false;
        $hasEmptyArray = false;

        foreach ($this->uniqueValuesType->all() as $nextType) {
            if ($nextType instanceof NullType) {
                $nullable = true;
            } elseif ($nextType instanceof EmptyArrayType) {
                $hasEmptyArray = true;
            } elseif ($nextType instanceof ArrayType) {
                $hasUntypedArray = true;
            }
        }

        if (null === $type && !$hasUntypedArray && !$hasEmptyArray) {
            return $nullable ? type_null() : type_optional(type_string());
        }

        // An array<mixed> value fits no narrower type; [] fits any list or map but not a structure.
        if ($hasUntypedArray || null === $type || $hasEmptyArray && !$type->isValid([])) {
            $type = type_array();
        }

        return $nullable ? type_optional($type) : $type;
    }
}
