<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Definition;

use DateTimeInterface;
use Flow\ETL\Schema\Definition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureElement;
use Flow\Types\Type\Logical\StructureType;

use function array_filter;

final readonly class ZoneAlignment
{
    /**
     * @param Definition<mixed> $definition
     */
    public function align(Definition $definition, mixed $value): mixed
    {
        return match (true) {
            $value === null => null,
            $this->inZone($definition, $value) => $value,
            default => $definition->type()->cast($value),
        };
    }

    /**
     * @param Type<mixed> $type
     */
    public function carriesDateTime(Type $type): bool
    {
        return match (true) {
            $type instanceof DateTimeType => true,
            $type instanceof OptionalType => $this->carriesDateTime($type->base()),
            $type instanceof ListType => $this->carriesDateTime($type->element()),
            $type instanceof MapType => $this->carriesDateTime($type->value()),
            $type instanceof StructureType
                => array_filter($type->elements(), fn(StructureElement $element): bool => $this->carriesDateTime($element->type))
                !== [],
            default => false,
        };
    }

    /**
     * @param array<string, Definition<mixed>> $definitions
     *
     * @return array<string, Definition<mixed>>
     */
    public function columns(array $definitions): array
    {
        $zoned = [];

        foreach ($definitions as $name => $definition) {
            if ($this->carriesDateTime($definition->type())) {
                $zoned[$name] = $definition;
            }
        }

        return $zoned;
    }

    /**
     * @param Definition<mixed> $definition
     */
    public function inZone(Definition $definition, mixed $value): bool
    {
        return (
            $definition instanceof DateTimeDefinition
            && $value instanceof DateTimeInterface
            && $value->getTimezone()->getName() === $definition->type()->zoneName()
        );
    }
}
