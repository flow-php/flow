<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\InstanceOfType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\TypeDetector;
use TypeError;

use function array_values;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\html_element_entry;
use function Flow\ETL\DSL\html_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\json_object_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\string_entry;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\structure_entry;
use function Flow\ETL\DSL\time_entry;
use function Flow\ETL\DSL\uuid_entry;
use function Flow\ETL\DSL\xml_element_entry;
use function Flow\ETL\DSL\xml_entry;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

final readonly class EntryFactory
{
    private EntryTypeResolver $typeResolver;

    public function __construct()
    {
        $this->typeResolver = new EntryTypeResolver();
    }

    /**
     * @param null|Definition<mixed>|Schema $schema
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Entry<mixed>
     */
    public function create(string $entryName, mixed $value, Schema|Definition|null $schema = null): Entry
    {
        if ($schema instanceof Definition) {
            return $this->createAs(
                $schema->entry()->name(),
                $value,
                $this->typeResolver->fromDefinition($schema),
                $schema->metadata(),
            );
        }

        if ($schema instanceof Schema) {
            $definition = $schema->get($entryName);

            return $this->createAs(
                $definition->entry()->name(),
                $value,
                $this->typeResolver->fromDefinition($definition),
                $definition->metadata(),
            );
        }

        if (null === $value) {
            return StringEntry::fromNull($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        return $this->createAs($entryName, $value, $valueType);
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Entry<mixed>
     */
    public function createAs(string $entryName, mixed $value, Type $type, ?Metadata $metadata = null): Entry
    {
        if (null === $value && $type instanceof OptionalType) {
            return match ($type->base()::class) {
                StringType::class => string_entry($entryName, null, $metadata),
                IntegerType::class => int_entry($entryName, null, $metadata),
                FloatType::class => float_entry($entryName, null, $metadata),
                BooleanType::class => bool_entry($entryName, null, $metadata),
                MapType::class => map_entry($entryName, null, $type->base(), $metadata),
                StructureType::class => struct_entry(
                    $entryName,
                    null,
                    type_instance_of(StructureType::class)->assert($type->base()),
                    $metadata,
                ),
                ListType::class => list_entry($entryName, null, $type->base(), $metadata),
                UuidType::class => uuid_entry($entryName, null, $metadata),
                DateTimeType::class => datetime_entry($entryName, null, $metadata),
                TimeType::class => time_entry($entryName, null, $metadata),
                DateType::class => date_entry($entryName, null, $metadata),
                EnumType::class => enum_entry($entryName, null, $metadata),
                ArrayType::class, JsonType::class => json_entry($entryName, null, $metadata),
                NullType::class => null_entry($entryName, $metadata),
                XMLType::class => xml_entry($entryName, null, $metadata),
                XMLElementType::class => xml_element_entry($entryName, null, $metadata),
                HTMLType::class => html_entry($entryName, null, $metadata),
                HTMLElementType::class => html_element_entry($entryName, null, $metadata),
                default => throw new InvalidArgumentException("Can't convert value into type \"{$type->toString()}\""),
            };
        }

        try {
            if ($type instanceof OptionalType) {
                $type = $type->base();
            }

            if ($type instanceof UnionType && $type->isOptionalType()) {
                $reduced = $type->types()->reduceOptionals()->first();

                if ($reduced === null) {
                    throw new InvalidArgumentException(
                        "Entry \"{$entryName}\": cannot reduce optional union type \"{$type->toString()}\".",
                    );
                }

                $type = $reduced;
            }

            if ($type instanceof UnionType) {
                $type = $this->typeResolver->fromUnion($type, $value, $entryName);
            }

            if ($type instanceof JsonType) {
                try {
                    return json_object_entry($entryName, type_optional($type)->cast($value), $metadata);
                } catch (InvalidArgumentException) {
                    return json_entry($entryName, type_optional($type)->cast($value), $metadata);
                }
            }

            return match ($type::class) {
                StringType::class => string_entry($entryName, type_optional($type)->cast($value), $metadata),
                IntegerType::class => int_entry($entryName, type_optional($type)->cast($value), $metadata),
                BooleanType::class => bool_entry($entryName, type_optional($type)->cast($value), $metadata),
                FloatType::class => float_entry($entryName, type_optional($type)->cast($value), $metadata),
                UuidType::class => uuid_entry($entryName, type_optional($type)->cast($value), $metadata),
                DateType::class => date_entry($entryName, type_optional($type)->cast($value), $metadata),
                TimeType::class => time_entry($entryName, type_optional($type)->cast($value), $metadata),
                DateTimeType::class => datetime_entry($entryName, type_optional($type)->cast($value), $metadata),
                TimeZoneType::class => string_entry($entryName, type_optional(type_string())->cast($value), $metadata),
                NullType::class => null_entry($entryName, $metadata),
                EnumType::class => enum_entry($entryName, type_optional($type)->cast($value), $metadata),
                HTMLType::class => html_entry($entryName, type_optional($type)->cast($value), $metadata),
                HTMLElementType::class => html_element_entry($entryName, type_optional($type)->cast($value), $metadata),
                XMLType::class => xml_entry($entryName, type_optional($type)->cast($value), $metadata),
                XMLElementType::class => xml_element_entry($entryName, type_optional($type)->cast($value), $metadata),
                ArrayType::class => json_entry($entryName, type_optional($type)->cast($value), $metadata),
                MapType::class => map_entry($entryName, $value === null ? null : $type->cast($value), $type, $metadata),
                StructureType::class => structure_entry(
                    $entryName,
                    $value === null ? null : $type->cast($value),
                    $type,
                    $metadata,
                ),
                ListType::class => list_entry(
                    $entryName,
                    $value === null ? null : array_values($type->cast($value)),
                    $type,
                    $metadata,
                ),
                InstanceOfType::class => throw new InvalidArgumentException(
                    "{$entryName}: {$type->toString()} can't be converted to any known Entry, please normalize that object first.",
                ),
                default => throw new InvalidArgumentException(
                    "Can't convert " . get_debug_type($value) . " value into type \"{$type->toString()}\"",
                ),
            };

            // @mago-ignore analysis:avoid-catching-error
        } catch (InvalidArgumentException|CastingException|TypeError $e) {
            throw new InvalidArgumentException(
                "Entry \"{$entryName}\" conversion exception. {$e->getMessage()}",
                previous: $e,
            );
        }
    }
}
