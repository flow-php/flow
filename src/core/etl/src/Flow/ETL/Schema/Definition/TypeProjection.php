<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final readonly class TypeProjection
{
    /**
     * @param ListType<list<mixed>> $type
     *
     * @return ListType<list<mixed>>
     */
    public function list(ListType $type): ListType
    {
        $element = $this->project($type->element());

        return $element === $type->element() ? $type : type_list($element);
    }

    /**
     * @param MapType<array<array-key, mixed>> $type
     *
     * @return MapType<array<array-key, mixed>>
     */
    public function map(MapType $type): MapType
    {
        $value = $this->project($type->value());

        return $value === $type->value() ? $type : type_map($type->key(), $value);
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function project(Type $type): Type
    {
        if ($type instanceof EmptyArrayType || $type instanceof ArrayType) {
            return type_json();
        }

        if ($type instanceof OptionalType) {
            $base = $this->project($type->base());

            return $base === $type->base() ? $type : type_optional($base);
        }

        if ($type instanceof StructureType) {
            return $this->structure($type);
        }

        if ($type instanceof ListType) {
            return $this->list($type);
        }

        if ($type instanceof MapType) {
            return $this->map($type);
        }

        if ($type instanceof UnionType) {
            return $this->union($type);
        }

        return $type;
    }

    /**
     * @param StructureType<array<array-key, mixed>> $type
     *
     * @return StructureType<array<array-key, mixed>>
     */
    public function structure(StructureType $type): StructureType
    {
        $changed = false;
        $elements = [];
        $optionalElements = [];

        foreach ($type->elements() as $name => $element) {
            $elements[$name] = $this->project($element);

            if ($elements[$name] !== $element) {
                $changed = true;
            }
        }

        foreach ($type->optionalElements() as $name => $element) {
            $optionalElements[$name] = $this->project($element);

            if ($optionalElements[$name] !== $element) {
                $changed = true;
            }
        }

        return $changed ? type_structure($elements, $optionalElements, $type->allowsExtra()) : $type;
    }

    /**
     * @param UnionType<mixed, mixed> $type
     *
     * @return UnionType<mixed, mixed>
     */
    public function union(UnionType $type): UnionType
    {
        $changed = false;
        $members = [];

        foreach ($type->types()->all() as $member) {
            $projected = $this->project($member);

            if ($projected !== $member) {
                $changed = true;
            }

            $members[] = $projected;
        }

        if (!$changed) {
            return $type;
        }

        $union = null;

        foreach ($members as $member) {
            $union = $union === null ? $member : new UnionType($union, $member);
        }

        return $union instanceof UnionType ? $union : $type;
    }
}
