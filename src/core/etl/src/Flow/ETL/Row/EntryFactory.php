<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

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
    type_date,
    type_datetime,
    type_float,
    type_int,
    type_json,
    type_string,
    type_time,
    type_uuid,
    type_xml,
    type_xml_element,
    uuid_entry,
    xml_element_entry,
    xml_entry};
use Flow\ETL\Exception\{CastingException,
    InvalidArgumentException,
    RuntimeException,
    SchemaDefinitionNotFoundException};
use Flow\ETL\PHP\Type\Caster\StringCastingHandler\StringTypeChecker;
use Flow\ETL\PHP\Type\{Caster, Type, TypeDetector};
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
use Flow\ETL\Schema;
use Flow\ETL\Schema\{Definition, Metadata};

final readonly class EntryFactory
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
            $definition = $schema->get($entryName);

            return $this->createAs($definition->entry()->name(), $value, $definition->type(), $definition->metadata());
        }

        if (null === $value) {
            return Entry\StringEntry::fromNull($entryName);
        }

        $valueType = (new TypeDetector())->detectType($value);

        if ($valueType instanceof StringType) {
            $stringChecker = new StringTypeChecker($value);

            if ($stringChecker->isJson()) {
                $valueType = type_json($valueType->nullable());
            }

            if ($stringChecker->isUuid()) {
                $valueType = type_uuid($valueType->nullable());
            }

            if ($stringChecker->isXML()) {
                $valueType = type_xml($valueType->nullable());
            }
        }

        if ($valueType instanceof ObjectType) {
            if ($valueType->class === \DOMDocument::class) {
                $valueType = type_xml($valueType->nullable());
            } elseif ($valueType->class === \DOMElement::class) {
                $valueType = type_xml_element($valueType->nullable());
            } elseif ($valueType->class === \DateInterval::class) {
                $valueType = type_time($valueType->nullable());
            } elseif (\in_array($valueType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                if ($value->format('H:i:s') === '00:00:00') {
                    $valueType = type_date($valueType->nullable());
                } else {
                    $valueType = type_datetime($valueType->nullable());
                }
            } else {
                foreach ([\Ramsey\Uuid\UuidInterface::class, \Flow\ETL\PHP\Value\Uuid::class, \Symfony\Component\Uid\Uuid::class] as $uuidClass) {
                    if (\is_a($valueType->class, $uuidClass, true)) {
                        $valueType = type_uuid($valueType->nullable());

                        break;
                    }
                }
            }
        }

        return $this->createAs($entryName, $value, $valueType);
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
                StringType::class => str_entry($entryName, null, $metadata),
                IntegerType::class => int_entry($entryName, null, $metadata),
                FloatType::class => float_entry($entryName, null, $metadata),
                BooleanType::class => bool_entry($entryName, null, $metadata),
                MapType::class => map_entry($entryName, null, $type, $metadata),
                StructureType::class => struct_entry($entryName, null, $type, $metadata),
                ListType::class => new Entry\ListEntry($entryName, null, $type, $metadata),
                UuidType::class => uuid_entry($entryName, null, $metadata),
                DateTimeType::class => datetime_entry($entryName, null, $metadata),
                TimeType::class => time_entry($entryName, null, $metadata),
                DateType::class => date_entry($entryName, null, $metadata),
                EnumType::class => enum_entry($entryName, null, $metadata),
                ArrayType::class, JsonType::class => json_entry($entryName, null, $metadata),
                NullType::class => Entry\StringEntry::fromNull($entryName, $metadata),
                XMLType::class => xml_entry($entryName, null, $metadata),
                XMLElementType::class => xml_element_entry($entryName, null, $metadata),
                default => throw new InvalidArgumentException("Can't convert value into type \"{$type->toString()}\""),
            };
        }

        try {
            if ($type instanceof StringType) {
                return str_entry($entryName, is_type([type_string()], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof IntegerType) {
                return int_entry($entryName, is_type([type_int()], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof BooleanType) {
                return bool_entry($entryName, is_type([type_boolean()], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof FloatType) {
                return float_entry($entryName, is_type([type_float()], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof UuidType) {
                return uuid_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof DateType) {
                return date_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof TimeType) {
                return time_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof DateTimeType) {
                return datetime_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof EnumType) {
                return enum_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof JsonType) {
                try {
                    return json_object_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
                } catch (InvalidArgumentException) {
                    return json_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
                }
            }

            if ($type instanceof XMLType) {
                return xml_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof XMLElementType) {
                return xml_element_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
            }

            if ($type instanceof ArrayType) {
                return json_entry($entryName, is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $metadata);
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
        } catch (InvalidArgumentException|CastingException|\TypeError $e) {
            throw new InvalidArgumentException("Entry \"{$entryName}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        throw new InvalidArgumentException("Can't convert value into type \"{$type->toString()}\"");
    }
}
