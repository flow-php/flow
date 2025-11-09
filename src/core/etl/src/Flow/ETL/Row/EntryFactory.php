<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use function Flow\ETL\DSL\{bool_entry,
    date_entry,
    datetime_entry,
    enum_entry,
    float_entry,
    html_element_entry,
    html_entry,
    int_entry,
    json_entry,
    json_object_entry,
    list_entry,
    map_entry,
    string_entry,
    struct_entry,
    time_entry,
    uuid_entry,
    xml_element_entry,
    xml_entry};
use function Flow\Types\DSL\{type_optional, type_string};
use Flow\ETL\Exception\{InvalidArgumentException, SchemaDefinitionNotFoundException};
use Flow\ETL\Row\Entry\{ListEntry, MapEntry, StringEntry, StructureEntry};
use Flow\ETL\Schema;
use Flow\ETL\Schema\{Definition, Metadata};
use Flow\Types\Exception\CastingException;
use Flow\Types\Type;
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    HTMLElementType,
    HTMLType,
    InstanceOfType,
    JsonType,
    ListType,
    MapType,
    OptionalType,
    StructureType,
    TimeType,
    TimeZoneType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\Types\Type\Native\{
    ArrayType,
    BooleanType,
    EnumType,
    FloatType,
    IntegerType,
    NullType,
    StringType,
    UnionType
};
use Flow\Types\Type\TypeDetector;

final readonly class EntryFactory
{
    /**
     * @param null|Definition<mixed>|Schema $schema
     *
     * @throws InvalidArgumentException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Entry<mixed>
     */
    public function create(string $entryName, mixed $value, Schema|Definition|null $schema = null) : Entry
    {
        if ($schema instanceof Definition) {
            return $this->createAs($schema->entry()->name(), $value, $schema, $schema->metadata());
        }

        if ($schema instanceof Schema) {
            $definition = $schema->get($entryName);

            return $this->createAs($definition->entry()->name(), $value, $definition, $definition->metadata());
        }

        if (null === $value) {
            return StringEntry::fromNull($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        return $this->createAs($entryName, $value, $valueType);
    }

    /**
     * @param Definition<mixed>|Type<mixed> $definition
     *
     * @return Entry<mixed>
     */
    public function createAs(string $entryName, mixed $value, Definition|Type $definition, ?Metadata $metadata = null) : Entry
    {
        if ($definition instanceof Definition) {
            if ($definition->isNullable()) {
                $type = type_optional($definition->type());
            } else {
                $type = $definition->type();
            }
        } else {
            $type = $definition;
        }

        if (null === $value && $type instanceof OptionalType) {
            return match ($type->base()::class) {
                StringType::class => string_entry($entryName, null, $metadata),
                IntegerType::class => int_entry($entryName, null, $metadata),
                FloatType::class => float_entry($entryName, null, $metadata),
                BooleanType::class => bool_entry($entryName, null, $metadata),
                MapType::class => map_entry($entryName, null, $type->base(), $metadata),
                StructureType::class => struct_entry($entryName, null, $type->base(), $metadata),
                ListType::class => list_entry($entryName, null, $type->base(), $metadata),
                UuidType::class => uuid_entry($entryName, null, $metadata),
                DateTimeType::class => datetime_entry($entryName, null, $metadata),
                TimeType::class => time_entry($entryName, null, $metadata),
                DateType::class => date_entry($entryName, null, $metadata),
                EnumType::class => enum_entry($entryName, null, $metadata),
                ArrayType::class, JsonType::class => json_entry($entryName, null, $metadata),
                NullType::class => StringEntry::fromNull($entryName, $metadata),
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
                $type = $type->types()->reduceOptionals()->first();
            }

            if ($type instanceof StringType) {
                return string_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof IntegerType) {
                return int_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof BooleanType) {
                return bool_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof FloatType) {
                return float_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof UuidType) {
                return uuid_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof DateType) {
                return date_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof TimeType) {
                return time_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof DateTimeType) {
                return datetime_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof TimeZoneType) {
                return string_entry($entryName, type_optional(type_string())->cast($value), $metadata);
            }

            if ($type instanceof NullType) {
                return StringEntry::fromNull($entryName, $metadata);
            }

            if ($type instanceof EnumType) {
                $castValue = type_optional($type)->cast($value);

                return enum_entry($entryName, (\is_object($castValue) && $castValue instanceof \UnitEnum) ? $castValue : null, $metadata);
            }

            if ($type instanceof JsonType) {
                try {
                    return json_object_entry($entryName, type_optional($type)->cast($value), $metadata);
                } catch (InvalidArgumentException) {
                    return json_entry($entryName, type_optional($type)->cast($value), $metadata);
                }
            }

            if ($type instanceof HTMLType) {
                return html_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof HTMLElementType) {
                return html_element_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof XMLType) {
                return xml_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof XMLElementType) {
                return xml_element_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof ArrayType) {
                return json_entry($entryName, type_optional($type)->cast($value), $metadata);
            }

            if ($type instanceof InstanceOfType) {
                throw new InvalidArgumentException("{$entryName}: {$type->toString()} can't be converted to any known Entry, please normalize that object first.");
            }

            if ($type instanceof MapType) {
                $processedValue = ($value === null) ? null : $type->cast($value);

                return new MapEntry($entryName, $processedValue, $type, $metadata);
            }

            if ($type instanceof StructureType) {
                $processedValue = ($value === null) ? null : $type->cast($value);

                return new StructureEntry($entryName, $processedValue, $type, $metadata);
            }

            if ($type instanceof ListType) {
                $processedValue = ($value === null) ? null : $type->cast($value);

                return new ListEntry($entryName, $processedValue, $type, $metadata);
            }
        } catch (InvalidArgumentException|CastingException|\TypeError $e) {
            throw new InvalidArgumentException("Entry \"{$entryName}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        /** @var Type<mixed> $type */
        throw new InvalidArgumentException("Can't convert " . get_debug_type($value) . " value into type \"{$type->toString()}\"");
    }
}
