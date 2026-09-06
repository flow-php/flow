<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\UnionType;
use UnitEnum;

use function array_key_exists;
use function array_values;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_uuid;
use function sprintf;

final readonly class InferredTypes
{
    /**
     * Keyed by class so a parameterised type (enum<A>) admits any member of its family.
     *
     * @var array<class-string<Type<mixed>>, Type<mixed>>
     */
    private array $types;

    /**
     * @param Type<mixed> ...$types
     */
    public function __construct(Type ...$types)
    {
        $byClass = [];

        foreach ($types as $type) {
            if (
                $type instanceof ListType
                || $type instanceof MapType
                || $type instanceof StructureType
                || $type instanceof UnionType
                || $type instanceof MixedType
                || $type instanceof NullType
            ) {
                throw new InvalidArgumentException(sprintf(
                    'Type "%s" cannot be inferred from a candidate list - container shape is decided from the '
                    . 'value, and null is never a column type. Candidates are leaf types, e.g. type_integer().',
                    $type->toString(),
                ));
            }

            $byClass[$type::class] = $type;
        }

        if ($byClass === []) {
            throw new InvalidArgumentException(
                'At least one type is required; pass type_string() for the all-strings mode.',
            );
        }

        // every value casts into string, so it is the floor no list can remove
        $byClass[type_string()::class] = type_string();

        $this->types = $byClass;
    }

    /**
     * Every rung but html and xml: no engine infers markup, HTMLType::cast() throws below PHP 8.4, and isXML()
     * builds a DOMDocument per cell.
     *
     * type_enum(UnitEnum::class) is the family marker, not a rung: no text source ever climbs to an enum, but a
     * value-typed source reads one straight off the value (InstanceOfTypeNarrower), and without the entry
     * TypeFloor would floor it to string - which StringType::cast() then refuses, making an enum column
     * unreadable. An explicit ->types(...) set that omits it still floors enums, which is what allStrings() is.
     */
    public static function default(): self
    {
        return new self(
            type_json(),
            type_uuid(),
            type_float(),
            type_integer(),
            type_datetime(),
            type_date(),
            type_boolean(),
            type_time_zone(),
            type_enum(UnitEnum::class),
            type_string(),
        );
    }

    /**
     * @param Type<mixed> $type
     */
    public function allows(Type $type): bool
    {
        return array_key_exists($type::class, $this->types);
    }

    /**
     * @return list<Type<mixed>>
     */
    public function toArray(): array
    {
        return array_values($this->types);
    }
}
