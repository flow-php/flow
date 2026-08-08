<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\TypeDetector;
use TypeError;

use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\Types\DSL\type_equals;
use function Flow\Types\DSL\type_string;

final class EntryFactory
{
    private readonly Instantiators $instantiators;

    private readonly EntryTypeResolver $typeResolver;

    private readonly TypeDetector $typeDetector;

    public function __construct()
    {
        $this->instantiators = new Instantiators();
        $this->typeResolver = new EntryTypeResolver();
        $this->typeDetector = new TypeDetector();
    }

    /**
     * @param null|Type<mixed> $type
     *
     * @return Entry<mixed>
     */
    public function create(string $name, mixed $value, ?Type $type = null, ?Metadata $metadata = null): Entry
    {
        if ($type === null) {
            if ($value === null) {
                return new NullEntry($name, $metadata);
            }

            return $this->build($name, $value, $this->typeDetector->detectType($value), $metadata, cast: true);
        }

        return $this->build($name, $value, $type, $metadata, cast: false);
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Entry<mixed>
     */
    public function cast(string $name, mixed $value, Type $type, ?Metadata $metadata = null): Entry
    {
        return $this->build($name, $value, $type, $metadata, cast: true);
    }

    /**
     * @param Definition<mixed> $definition
     *
     * @return Entry<mixed>
     */
    public function fromDefinition(Definition $definition, mixed $value): Entry
    {
        if ($definition instanceof UnionDefinition) {
            $definition = $definition->memberFor($value);
        }

        $variant = $value === null && !$definition->isNullable() ? $definition->makeNullable() : $definition;

        return $this->instantiators->for($variant->entryClass())->instantiate(
            $variant->entry()->name(),
            $value,
            $variant,
        );
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Entry<mixed>
     */
    private function build(string $name, mixed $value, Type $type, ?Metadata $metadata, bool $cast): Entry
    {
        $declaredType = $type;

        try {
            if ($type instanceof OptionalType) {
                $type = $type->base();
            }

            if ($type instanceof UnionType && $type->isOptionalType()) {
                $reduced = $type->types()->reduceOptionals()->first();

                if ($reduced === null) {
                    throw new InvalidArgumentException(
                        "Entry \"{$name}\": cannot reduce optional union type \"{$type->toString()}\".",
                    );
                }

                $type = $reduced;
            }

            if ($type instanceof UnionType) {
                $type = $this->typeResolver->fromUnion($type, $value, $name);
            }

            if ($type instanceof NullType) {
                return new NullEntry($name, $metadata);
            }

            if ($type instanceof InstanceOfType) {
                throw new InvalidArgumentException(
                    "{$name}: {$type->toString()} can't be converted to any known Entry, please normalize that object first.",
                );
            }

            if ($type instanceof TimeZoneType) {
                $type = type_string();
            }

            $definition = definition_from_type($name, $type, $value === null, $metadata);

            return $this->fromDefinition($definition, $this->prepareValue(
                $value,
                $declaredType,
                $definition->type(),
                $cast,
            ));

            // @mago-ignore analysis:avoid-catching-error
        } catch (InvalidArgumentException|CastingException|TypeError $e) {
            throw new InvalidArgumentException(
                "Entry \"{$name}\" conversion exception. {$e->getMessage()}",
                previous: $e,
            );
        }
    }

    /**
     * @param Type<mixed> $declaredType
     * @param Type<mixed> $entryType
     */
    private function prepareValue(mixed $value, Type $declaredType, Type $entryType, bool $cast): mixed
    {
        if ($value === null) {
            return $value;
        }

        if (!$cast && type_equals($declaredType, $entryType)) {
            return $value;
        }

        if ($entryType instanceof ListType) {
            return array_values($entryType->cast($value));
        }

        return $entryType->cast($value);
    }
}
