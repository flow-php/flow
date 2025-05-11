<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter\PHPFormatter;

use Flow\ETL\PHP\Type\Logical\{DateTimeType,
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
use Flow\ETL\PHP\Type\Native\{ArrayType, BooleanType, CallableType, FloatType, IntegerType, NullType, ResourceType, StringType};
use Flow\ETL\PHP\Type\Type;

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
            ArrayType::class => $this->formatArrayType($type, $nullable),
            OptionalType::class => $this->format($type->base(), true),
            default => $this->formatSimpleType($type, $nullable),
        };
    }

    private function formatArrayType(ArrayType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_array');

        return \sprintf(
            '\%s(nullable: %s)',
            $reflection->getName(),
            $nullable ? 'true' : 'false'
        );
    }

    private function formatListType(ListType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_list');

        return \sprintf(
            '\%s(element: %s, nullable: %s)',
            $reflection->getName(),
            $this->format($type->element()),
            $nullable ? 'true' : 'false'
        );
    }

    private function formatMapType(MapType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_map');

        return \sprintf(
            '\%s(key_type: %s, value_type: %s, nullable: %s)',
            $reflection->getName(),
            $this->format($type->key()),
            $this->format($type->value()),
            $nullable ? 'true' : 'false'
        );
    }

    /**
     * @param Type<mixed> $type
     */
    private function formatSimpleType(Type $type, bool $nullable) : string
    {
        $reflection = match ($type::class) {
            StringType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_string'),
            IntegerType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_integer'),
            BooleanType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_boolean'),
            FloatType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_float'),
            DateTimeType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_datetime'),
            DateType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_date'),
            TimeType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_time'),
            ResourceType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_resource'),
            NullType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_null'),
            UuidType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_uuid'),
            CallableType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_callable'),
            JsonType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_json'),
            XMLType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_xml'),
            XMLElementType::class => new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_xml_element'),
            default => throw new \RuntimeException('Type ' . $type->toString() . ' is not a simple definition'),
        };

        if ($type instanceof NullType) {
            return \sprintf('\%s()', $reflection->getName());
        }

        return \sprintf(
            '\%s(nullable: %s)',
            $reflection->getName(),
            $nullable ? 'true' : 'false'
        );
    }

    private function formatStructureType(StructureType $type, bool $nullable) : string
    {
        $reflection = new \ReflectionFunction('\\Flow\\ETL\\DSL\\type_structure');

        $fields = [];

        foreach ($type->elements() as $name => $element) {
            $fields[] = \sprintf('"%s" => %s', $name, $this->format($element));
        }

        return \sprintf(
            '\%s(elements: [%s], nullable: %s)',
            $reflection->getName(),
            \implode(', ', $fields),
            $nullable ? 'true' : 'false'
        );
    }
}
