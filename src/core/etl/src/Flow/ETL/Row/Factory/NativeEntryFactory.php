<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Factory;

use function Flow\ETL\DSL\{array_entry,
    bool_entry,
    datetime_entry,
    enum_entry,
    float_entry,
    int_entry,
    is_type,
    json_entry,
    json_object_entry,
    map_entry,
    obj_entry,
    object_entry,
    str_entry,
    struct_entry,
    type_boolean,
    type_float,
    type_int,
    type_string,
    uuid_entry,
    xml_element_entry,
    xml_entry};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException, SchemaDefinitionNotFoundException};
use Flow\ETL\PHP\Type\Caster\StringCastingHandler\StringTypeChecker;
use Flow\ETL\PHP\Type\Logical\{DateTimeType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\ETL\PHP\Type\Native\{ArrayType, EnumType, ObjectType, ScalarType};
use Flow\ETL\PHP\Type\{Caster, TypeDetector};
use Flow\ETL\Row\{Entry, EntryFactory, Schema, Schema\Definition};
use Ramsey\Uuid\UuidInterface;
use Symfony\Component\Uid\Uuid;

final class NativeEntryFactory implements EntryFactory
{
    private readonly Caster $caster;

    public function __construct()
    {
        $this->caster = Caster::default();
    }

    /**
     * @throws InvalidArgumentException
     * @throws RuntimeException
     * @throws SchemaDefinitionNotFoundException
     */
    public function create(string $entryName, mixed $value, Schema|Definition|null $schema = null) : Entry
    {
        if ($schema instanceof Definition) {
            return $this->fromDefinition($schema, $value);
        }

        if ($schema instanceof Schema) {
            return $this->fromDefinition($schema->getDefinition($entryName), $value);
        }

        if (null === $value) {
            return new Entry\StringEntry($entryName, null);
        }

        $valueType = (new TypeDetector())->detectType($value);

        if ($valueType instanceof ScalarType) {
            if ($valueType->isString()) {
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

            if ($valueType->isFloat()) {
                return float_entry($entryName, $value);
            }

            if ($valueType->isInteger()) {
                return int_entry($entryName, $value);
            }

            if ($valueType->isBoolean()) {
                return bool_entry($entryName, $value);
            }
        }

        if ($valueType instanceof JsonType) {
            return json_entry($entryName, $value);
        }

        if ($valueType instanceof UuidType) {
            if ($value instanceof \Flow\ETL\PHP\Value\Uuid) {
                return uuid_entry($entryName, $value);
            }

            return uuid_entry($entryName, (string) $value);
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

            if (\in_array($valueType->class, [\DateTimeImmutable::class, \DateTimeInterface::class, \DateTime::class], true)) {
                return datetime_entry($entryName, $value);
            }

            if (\in_array($valueType->class, [\Flow\ETL\PHP\Value\Uuid::class, UuidInterface::class, Uuid::class], true)) {
                if (\in_array($valueType->class, [UuidInterface::class, Uuid::class], true)) {
                    return uuid_entry($entryName, new \Flow\ETL\PHP\Value\Uuid($value));
                }

                return uuid_entry($entryName, $value);
            }

            return object_entry($entryName, $value);
        }

        if ($valueType instanceof EnumType) {
            return enum_entry($entryName, $value);
        }

        if ($valueType instanceof ArrayType) {
            return array_entry($entryName, $value);
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

    private function fromDefinition(Definition $definition, mixed $value) : Entry
    {
        $type = $definition->type();

        if (null === $value && $definition->isNullable()) {
            return match ($type::class) {
                ScalarType::class => match ($type->type()) {
                    ScalarType::STRING => str_entry($definition->entry()->name(), null),
                    ScalarType::INTEGER => int_entry($definition->entry()->name(), null),
                    ScalarType::FLOAT => float_entry($definition->entry()->name(), null),
                    ScalarType::BOOLEAN => bool_entry($definition->entry()->name(), null),
                    default => throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\""),
                },
                ObjectType::class => obj_entry($definition->entry()->name(), null),
                ArrayType::class => array_entry($definition->entry()->name(), null),
                MapType::class => map_entry($definition->entry()->name(), null, $type),
                StructureType::class => struct_entry($definition->entry()->name(), null, $type),
                ListType::class => new Entry\ListEntry($definition->entry()->name(), null, $type),
                UuidType::class => uuid_entry($definition->entry()->name(), null),
                DateTimeType::class => datetime_entry($definition->entry()->name(), null),
                EnumType::class => enum_entry($definition->entry()->name(), null),
                JsonType::class => json_entry($definition->entry()->name(), null),
                default => throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\""),
            };
        }

        try {
            if ($type instanceof ScalarType) {
                return match ($type->type()) {
                    ScalarType::STRING => str_entry($definition->entry()->name(), is_type([type_string()], $value) ? $value : $this->caster->to($type)->value($value)),
                    ScalarType::INTEGER => int_entry($definition->entry()->name(), is_type([type_int()], $value) ? $value : $this->caster->to($type)->value($value)),
                    ScalarType::FLOAT => float_entry($definition->entry()->name(), is_type([type_float()], $value) ? $value : $this->caster->to($type)->value($value)),
                    ScalarType::BOOLEAN => bool_entry($definition->entry()->name(), is_type([type_boolean()], $value) ? $value : $this->caster->to($type)->value($value)),
                    default => throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\""),
                };
            }

            if ($type instanceof XMLType) {
                return xml_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof UuidType) {
                return uuid_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof DateTimeType) {
                return datetime_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof EnumType) {
                return enum_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof JsonType) {
                try {
                    return json_object_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
                } catch (InvalidArgumentException) {
                    return json_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
                }
            }

            if ($type instanceof ObjectType) {
                return obj_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof ArrayType) {
                return array_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value));
            }

            if ($type instanceof MapType) {
                return map_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type);
            }

            if ($type instanceof StructureType) {
                return struct_entry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type);
            }

            if ($type instanceof ListType) {
                return new Entry\ListEntry($definition->entry()->name(), is_type([$type], $value) ? $value : $this->caster->to($type)->value($value), $type);
            }
        } catch (InvalidArgumentException|\TypeError $e) {
            throw new InvalidArgumentException("Field \"{$definition->entry()}\" conversion exception. {$e->getMessage()}", previous: $e);
        }

        throw new InvalidArgumentException("Can't convert value into entry \"{$definition->entry()}\"");
    }
}
