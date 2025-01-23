<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use function Flow\ETL\DSL\{bool_entry,
    date_entry,
    datetime_entry,
    enum_entry,
    float_entry,
    int_entry,
    is_type,
    json_entry,
    json_object_entry,
    map_entry,
    str_entry,
    struct_entry,
    time_entry,
    type_boolean,
    type_float,
    type_int,
    type_json,
    type_string,
    uuid_entry,
    xml_element_entry,
    xml_entry};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException, SchemaDefinitionNotFoundException};
use Flow\ETL\PHP\Type\Caster\StringCastingHandler\StringTypeChecker;
use Flow\ETL\PHP\Type\Logical\{DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType,
    BooleanType,
    EnumType,
    FloatType,
    IntegerType,
    NullType,
    ObjectType,
    StringType};
use Flow\ETL\PHP\Type\{Caster, Type, TypeDetector};
use Flow\ETL\Row\{Entry, EntryFactory, Schema, Schema\Definition, Schema\Metadata};
use Ramsey\Uuid\UuidInterface;
use Symfony\Component\Uid\Uuid;

final readonly class NativeEntryFactory implements EntryFactory
{
    private Caster $caster;

    public function __construct()
    {
        $this->caster = Caster::default();
    }

    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     * @throws SchemaDefinitionNotFoundException
     *
     * @return Entry<mixed, mixed>
     */
    public function create(string $entryName, mixed $value, Schema|Definition|null $schema = null) : Entry
    {
        if ($schema instanceof Definition) {
            return $this->createAs($schema->entry()->name(), $value, $schema->type(), $schema->metadata());
        }

        if ($schema instanceof Schema) {
            $definition = $schema->getDefinition($entryName);

            return $this->createAs($definition->entry()->name(), $value, $definition->type(), $definition->metadata());
        }

        if (null === $value) {
            return Entry\StringEntry::fromNull($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        if ($valueType instanceof StringType) {
            $stringChecker = new StringTypeChecker($value);

            if ($stringChecker->isJson()) {
                return json_entry($entryName, $value);
            }

            if ($stringChecker->isUuid()) {
                return uuid_entry($entryName, \Flow\ETL\PHP\Value\Uuid::fromString($value));
            }

            if ($stringChecker->isXML()) {
                return xml_entry($entryName, $value);
            }

            return str_entry($entryName, $value);
        }

        if ($valueType instanceof FloatType) {
            return float_entry($entryName, $value, $valueType->precision);
        }

        if ($valueType instanceof IntegerType) {
            return int_entry($entryName, $value);
        }

        if ($valueType instanceof BooleanType) {
            return bool_entry($entryName, $value);
        }

        if ($valueType instanceof JsonType || $valueType instanceof ArrayType) {
            return json_entry($entryName, $value);
        }

        if ($valueType instanceof UuidType) {
            if ($value instanceof \Flow\ETL\PHP\Value\Uuid) {
                return uuid_entry($entryName, $value);
            }

            return uuid_entry($entryName, (string) $value);
        }

        if ($valueType instanceof TimeType) {
            return time_entry($entryName, $value);
        }

        if ($valueType instanceof DateType) {
            return date_entry($entryName, $value);
        }

        if ($valueType instanceof DateTimeType) {
            return datetime_entry($entryName, $value);
        }

        if ($valueType instanceof XMLType) {
            return xml_entry($entryName, $value);
        }

        if ($valueType instanceof XMLElementType) {
            return xml_element_entry($entryName, $value);
        }

        if ($valueType instanceof ObjectType) {
            if ($valueType->class === \DOMDocument::class) {
                return xml_entry($entryName, $value);
            }

            if ($valueType->class === \DOMElement::class) {
                return xml_element_entry($entryName, $value);
            }

            if ($valueType->class === \DateInterval::class) {
                return time_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                if ($value->format('H:i:s') === '00:00:00') {
                    return date_entry($entryName, $value);
                }

                return datetime_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [\Flow\ETL\PHP\Value\Uuid::class, UuidInterface::class, Uuid::class], true)) {
                if (\in_array($valueType->class, [UuidInterface::class, Uuid::class], true)) {
                    return uuid_entry($entryName, new \Flow\ETL\PHP\Value\Uuid($value));
                }

                return uuid_entry($entryName, $value);
            }

            throw new InvalidArgumentException("{$entryName}: {$valueType->toString()} can't be converted to any known Entry, please normalize that object first.");
        }

        if ($valueType instanceof EnumType) {
            return enum_entry($entryName, $value);
        }

        if ($valueType instanceof ListType) {
            return new Entry\ListEntry($entryName, $value, $valueType);
        }

        if ($valueType instanceof MapType) {
            return new Entry\MapEntry($entryName, $value, $valueType);
        }

        if ($valueType instanceof StructureType) {
            return new Entry\StructureEntry($entryName, $value, $valueType);
        }

        throw new InvalidArgumentException("{$valueType->toString()} can't be converted to any known Entry");
    }

    /**
     * @param Type<mixed> $type
     *
     * @return Entry<mixed, mixed>
     */
    public function createAs(string $entryName, mixed $value, Type $type, ?Metadata $metadata = null) : Entry
    {
        if (null === $value && $type->nullable()) {
            return match ($type::class) {
                StringType::class => str_entry($entryName, null, $type, $metadata),
                IntegerType::class => int_entry($entryName, null, $type, $metadata),
                FloatType::class => float_entry($entryName, null, $type->precision, $type, $metadata),
                BooleanType::class => bool_entry($entryName, null, $type, $metadata),
                MapType::class => map_entry($entryName, null, $type, $metadata),
                StructureType::class => struct_entry($entryName, null, $type, $metadata),
                ListType::class => new Entry\ListEntry($entryName, null, $type, $metadata),
                UuidType::class => uuid_entry($entryName, null, $type, $metadata),
                DateTimeType::class => datetime_entry($entryName, null, $type, $metadata),
                TimeType::class => time_entry($entryName, null, $type, $metadata),
                DateType::class => date_entry($entryName, null, $type, $metadata),
                EnumType::class => enum_entry($entryName, null, $type, $metadata),
                ArrayType::class, JsonType::class => json_entry($entryName, null, type_json($type->nullable()), $metadata),
                NullType::class => Entry\StringEntry::fromNull($entryName, $metadata),
                default => throw new InvalidArgumentException("Can't convert value into type \"{$type->toString()}\""),
            };
        }

        try {
            if ($type instanceof StringType) {
                return str_entry($entryName, is_type([type_string()], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof IntegerType) {
                return int_entry($entryName, is_type([type_int()], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof BooleanType) {
                return bool_entry($entryName, is_type([type_boolean()], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof FloatType) {
                return float_entry($entryName, is_type([type_float()], $value) ? $value : $this->caster->to($type)->value($value), $type->precision, $type, $metadata);
            }

            if ($type instanceof XMLType) {
                return xml_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof UuidType) {
                return uuid_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof DateType) {
                return date_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof TimeType) {
                return time_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof DateTimeType) {
                return datetime_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof EnumType) {
                return enum_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof JsonType) {
                try {
                    return json_object_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
                } catch (InvalidArgumentException) {
                    return json_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
                }
            }

            if ($type instanceof ArrayType) {
                return json_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), type_json($type->nullable()), $metadata);
            }

            if ($type instanceof ObjectType) {
                throw new InvalidArgumentException("{$entryName}: {$type->toString()} can't be converted to any known Entry, please normalize that object first.");
            }

            if ($type instanceof MapType) {
                return map_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof StructureType) {
                return struct_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }

            if ($type instanceof ListType) {
                return new Entry\ListEntry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type, $metadata);
            }
        } catch (InvalidArgumentException|\TypeError $e) {
            throw new InvalidArgumentException("Field \"{$entryName}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        throw new InvalidArgumentException("Can't convert value into type \"{$type->toString()}\"");
    }
}
