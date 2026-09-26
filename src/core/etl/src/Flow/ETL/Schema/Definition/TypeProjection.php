<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use Flow\ETL\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Row\Reference;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;

final readonly class TypeProjection
{
    public function __construct(
        private Reference $column,
    ) {}

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
            // Iceberg's rule, as definition_from_type() applies it to the column itself
            if (!$type->isOptionalType()) {
                throw UnsupportedUnionTypeException::forElement($this->column, $type);
            }

            return type_optional($this->project(type_bare($type)));
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

        foreach ($type->elements() as $element) {
            $projected = $this->project($element->type);

            if ($projected !== $element->type) {
                $changed = true;
            }

            $elements[] = structure_element($element->name, $projected, $element->optional);
        }

        return $changed ? new StructureType($elements) : $type;
    }
}
