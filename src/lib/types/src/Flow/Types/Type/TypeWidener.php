<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function in_array;

final readonly class TypeWidener
{
    private const CONTAINERS = [
        ArrayType::class,
        EmptyArrayType::class,
        JsonType::class,
        ListType::class,
        MapType::class,
        StructureType::class,
    ];

    /**
     * @param Type<mixed> $left
     * @param Type<mixed> $right
     *
     * @return Type<mixed>
     */
    public function widen(Type $left, Type $right): Type
    {
        if (type_equals($left, $right)) {
            return $left;
        }

        if ($left instanceof NullType) {
            return $this->nullable($right);
        }

        if ($right instanceof NullType) {
            return $this->nullable($left);
        }

        if ($left instanceof OptionalType || $right instanceof OptionalType) {
            $base = $this->widen(
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
            return $this->widenStructures($left, $right);
        }

        if ($left instanceof ListType && $right instanceof ListType) {
            return $this->widenLists($left, $right);
        }

        if ($left instanceof MapType && $right instanceof MapType) {
            return $this->widenMaps($left, $right);
        }

        // json holds any container shape without flattening it to text, so it beats the string fallback below.
        if (in_array($left::class, self::CONTAINERS, true) && in_array($right::class, self::CONTAINERS, true)) {
            return type_json();
        }

        // Inference cannot throw, so an irreconcilable pair widens to the most permissive type instead.
        return type_string();
    }

    /**
     * `mixed` has no optional form - it already admits null - so wrapping it would throw from a
     * method that must always answer.
     *
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function nullable(Type $type): Type
    {
        if ($type instanceof OptionalType || $type instanceof MixedType) {
            return $type;
        }

        return type_optional($type);
    }

    /**
     * @template TLeft
     * @template TRight
     *
     * @param ListType<list<TLeft>> $left
     * @param ListType<list<TRight>> $right
     *
     * @return ListType<list<mixed>>
     */
    public function widenLists(ListType $left, ListType $right): ListType
    {
        return type_list($this->widen($left->element(), $right->element()));
    }

    /**
     * @template TLeftKey of array-key
     * @template TLeftValue
     * @template TRightKey of array-key
     * @template TRightValue
     *
     * @param MapType<array<TLeftKey, TLeftValue>> $left
     * @param MapType<array<TRightKey, TRightValue>> $right
     *
     * @return MapType<array<array-key, mixed>>
     */
    public function widenMaps(MapType $left, MapType $right): MapType
    {
        return type_map(
            type_equals($left->key(), $right->key()) ? $left->key() : type_string(),
            $this->widen($left->value(), $right->value()),
        );
    }

    /**
     * @template TLeft
     * @template TRight
     *
     * @param StructureType<array<array-key, TLeft>> $left
     * @param StructureType<array<array-key, TRight>> $right
     *
     * @return StructureType<array<array-key, mixed>>
     */
    public function widenStructures(StructureType $left, StructureType $right): StructureType
    {
        $rightByName = [];

        foreach ($right->elements() as $element) {
            $rightByName[$element->name] = $element;
        }

        $elements = [];

        foreach ($left->elements() as $element) {
            $other = $rightByName[$element->name] ?? null;
            unset($rightByName[$element->name]);

            $elements[] = $other === null
                ? structure_element($element->name, $element->type, optional: true)
                : structure_element(
                    $element->name,
                    $this->widen($element->type, $other->type),
                    $element->optional || $other->optional,
                );
        }

        foreach ($rightByName as $element) {
            $elements[] = structure_element($element->name, $element->type, optional: true);
        }

        return new StructureType($elements, $left->allowsExtra() || $right->allowsExtra());
    }
}
