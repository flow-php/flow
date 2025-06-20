<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter\PHPFormatter;

use Flow\Types\Type;
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    OptionalType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\Types\Type\Native\{ArrayType,
    BooleanType,
    CallableType,
    FloatType,
    IntegerType,
    NullType,
    ResourceType,
    StringType};

final class TypeFormatter
{
    /**
     * @param Type<mixed> $type
     */
    public function format(Type $type, bool $nullable = false) : string
    {
        return match ($type::class) {
            MapType::class => $this->formatMapType($type, $nullable),
            ListType::class => $this->formatListType($type, $nullable),
            StructureType::class => $this->formatStructureType($type, $nullable),
            OptionalType::class => $this->format($type->base(), true),
            default => $this->formatSimpleType($type, $nullable),
        };
    }

    /**
     * @param ListType<mixed> $type
     */
    private function formatListType(ListType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\Types\\DSL\\type_list');

        return \sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(%s(element: %s))' : '\%s(element: %s)',
            $reflection->getName(),
            $this->format($type->element()),
        );
    }

    /**
     * @param MapType<array-key, mixed> $type
     */
    private function formatMapType(MapType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\Types\\DSL\\type_map');

        return \sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(\%s(key_type: %s, value_type: %s))' : '\%s(key_type: %s, value_type: %s)',
            $reflection->getName(),
            $this->format($type->key()),
            $this->format($type->value()),
        );
    }

    /**
     * @param Type<mixed> $type
     */
    private function formatSimpleType(Type $type, bool $nullable) : string
    {
        $reflection = match ($type::class) {
            ArrayType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_array'),
            StringType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_string'),
            IntegerType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_integer'),
            BooleanType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_boolean'),
            FloatType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_float'),
            DateTimeType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_datetime'),
            DateType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_date'),
            TimeType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_time'),
            ResourceType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_resource'),
            NullType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_null'),
            UuidType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_uuid'),
            CallableType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_callable'),
            JsonType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_json'),
            XMLType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_xml'),
            XMLElementType::class => new \ReflectionFunction('\\Flow\\Types\\DSL\\type_xml_element'),
            default => throw new \RuntimeException('Type ' . $type->toString() . ' is not a simple definition'),
        };

        if ($type instanceof NullType) {
            return \sprintf('\%s()', $reflection->getName());
        }

        return \sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(\%s())' : '\%s()',
            $reflection->getName(),
        );
    }

    /**
     * @param StructureType<array<string, Type<mixed>>> $type
     */
    private function formatStructureType(StructureType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\Types\\DSL\\type_structure');

        $fields = [];

        foreach ($type->elements() as $name => $element) {
            $fields[] = \sprintf('"%s" => %s', $name, $this->format($element));
        }

        return \sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(\%s(elements: [%s]))' : '\%s(elements: [%s])',
            $reflection->getName(),
            \implode(', ', $fields),
        );
    }
}
