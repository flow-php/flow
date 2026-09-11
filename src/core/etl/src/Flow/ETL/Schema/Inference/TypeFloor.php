<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Schema\Definition\TypeProjection;
use Flow\Types\Type;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\UnionType;

use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final readonly class TypeFloor
{
    public function __construct(
        private InferredTypes $types,
        private TypeProjection $projection = new TypeProjection(),
    ) {}

    /**
     * project() only rewrites arrays to json, so the container arms below must recurse again to reach the leaves.
     *
     * @param Type<mixed> $type
     *
     * @return Type<mixed>
     */
    public function floor(Type $type): Type
    {
        $type = $this->projection->project($type);

        return match (true) {
            $type instanceof OptionalType => $this->optional($type),
            // [] and {} both decode to [], so list<null> is "some container", which only json holds
            $type instanceof ListType => $type->element() instanceof NullType
                ? type_json()
                : type_list($this->floor($type->element())),
            $type instanceof MapType => type_map($type->key(), $this->floor($type->value())),
            $type instanceof StructureType => $this->structure($type),
            $type instanceof NullType,
            $type instanceof InstanceOfType,
            $type instanceof MixedType,
            $type instanceof UnionType,
                => type_string(),
            !$this->types->allows($type) => type_string(),
            default => $type,
        };
    }

    /**
     * @param OptionalType<mixed> $type
     *
     * @return Type<mixed>
     */
    public function optional(OptionalType $type): Type
    {
        return type_optional($this->floor($type->base()));
    }

    /**
     * @param StructureType<array<array-key, mixed>> $type
     *
     * @return StructureType<array<array-key, mixed>>
     */
    public function structure(StructureType $type): StructureType
    {
        $elements = [];

        foreach ($type->elements() as $element) {
            // A column observed only as null floors to string and is then made nullable by the column rule
            // - every inferred column is nullable. A structure element has no such rule above it, so the floor
            // has to carry the nullability here, or the schema rejects the rows it was inferred from.
            $elements[] = structure_element(
                $element->name,
                $element->type instanceof NullType ? type_optional(type_string()) : $this->floor($element->type),
                $element->optional,
            );
        }

        return new StructureType($elements, $type->allowsExtra());
    }
}
