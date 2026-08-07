<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;

use function array_key_exists;
use function array_keys;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final readonly class TypeMerge
{
    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     *
     * @return Type<mixed>
     */
    public function merge(Type $left, Type $right): Type
    {
        if (type_equals($left, $right)) {
            return $left;
        }

        if ($left instanceof NullType) {
            return $right instanceof OptionalType ? $right : type_optional($right);
        }

        if ($right instanceof NullType) {
            return $left instanceof OptionalType ? $left : type_optional($left);
        }

        if ($left instanceof OptionalType || $right instanceof OptionalType) {
            $base = $this->merge(
                $left instanceof OptionalType ? $left->base() : $left,
                $right instanceof OptionalType ? $right->base() : $right,
            );

            return $base instanceof OptionalType ? $base : type_optional($base);
        }

        if (
            ($left instanceof IntegerType || $left instanceof FloatType)
            && ($right instanceof IntegerType || $right instanceof FloatType)
        ) {
            return type_float();
        }

        if (
            ($left instanceof DateType || $left instanceof DateTimeType)
            && ($right instanceof DateType || $right instanceof DateTimeType)
        ) {
            return type_datetime();
        }

        if ($left instanceof StructureType && $right instanceof StructureType) {
            return $this->mergeStructures($left, $right);
        }

        if ($left instanceof ListType && $right instanceof ListType) {
            return $this->mergeLists($left, $right);
        }

        if ($left instanceof MapType && $right instanceof MapType) {
            return $this->mergeMaps($left, $right);
        }

        // Inference cannot throw, so an irreconcilable pair widens to the most permissive type instead.
        return type_string();
    }

    /**
     * @template TLeft
     * @template TRight
     *
     * @param ListType<TLeft> $left
     * @param ListType<TRight> $right
     *
     * @return ListType<mixed>
     */
    public function mergeLists(ListType $left, ListType $right): ListType
    {
        return new ListType($this->merge($left->element(), $right->element()));
    }

    /**
     * @template TLeftKey of array-key
     * @template TLeftValue
     * @template TRightKey of array-key
     * @template TRightValue
     *
     * @param MapType<TLeftKey, TLeftValue> $left
     * @param MapType<TRightKey, TRightValue> $right
     *
     * @return MapType<array-key, mixed>
     */
    public function mergeMaps(MapType $left, MapType $right): MapType
    {
        // Map keys are always array-key, so the only reachable widening is integer and string to string.
        return new MapType(
            type_equals($left->key(), $right->key()) ? $left->key() : type_string(),
            $this->merge($left->value(), $right->value()),
        );
    }

    /**
     * @template TLeft
     * @template TRight
     *
     * @param StructureType<TLeft> $left
     * @param StructureType<TRight> $right
     *
     * @return StructureType<mixed>
     */
    public function mergeStructures(StructureType $left, StructureType $right): StructureType
    {
        $leftOptional = $left->optionalElements();
        $rightOptional = $right->optionalElements();
        $leftElements = $left->elements() + $leftOptional;
        $rightElements = $right->elements() + $rightOptional;

        $required = [];
        $optional = [];

        foreach ([...array_keys($leftElements), ...array_keys($rightElements)] as $name) {
            if (array_key_exists($name, $required) || array_key_exists($name, $optional)) {
                continue;
            }

            $inLeft = array_key_exists($name, $leftElements);
            $inRight = array_key_exists($name, $rightElements);

            if ($inLeft && $inRight) {
                $element = $this->merge($leftElements[$name], $rightElements[$name]);
                $isOptional = array_key_exists($name, $leftOptional) || array_key_exists($name, $rightOptional);
            } else {
                $element = $inLeft ? $leftElements[$name] : $rightElements[$name];
                $isOptional = true;
            }

            if ($isOptional) {
                $optional[$name] = $element;
            } else {
                $required[$name] = $element;
            }
        }

        return new StructureType($required, $optional, $left->allowsExtra() || $right->allowsExtra());
    }
}
