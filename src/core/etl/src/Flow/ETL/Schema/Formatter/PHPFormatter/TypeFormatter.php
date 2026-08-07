<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter\PHPFormatter;

use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\CallableType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\ResourceType;
use Flow\Types\Type\Native\StringType;
use ReflectionFunction;
use RuntimeException;

use function implode;
use function sprintf;

final class TypeFormatter
{
    /**
     * @param Type<mixed> $type
     */
    public function format(Type $type, bool $nullable = false): string
    {
        if ($type instanceof MapType) {
            return $this->formatMapType($type, $nullable);
        }

        if ($type instanceof ListType) {
            return $this->formatListType($type, $nullable);
        }

        if ($type instanceof StructureType) {
            return $this->formatStructureType($type, $nullable);
        }

        if ($type instanceof OptionalType) {
            return $this->format($type->base(), true);
        }

        return $this->formatSimpleType($type, $nullable);
    }

    /**
     * @param ListType<list<mixed>> $type
     */
    private function formatListType(ListType $type, bool $nullable): string
    {
        $reflection = new ReflectionFunction('\\Flow\\Types\\DSL\\type_list');

        return sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(%s(element: %s))' : '\%s(element: %s)',
            $reflection->getName(),
            $this->format($type->element()),
        );
    }

    /**
     * @param MapType<array<array-key, mixed>> $type
     */
    private function formatMapType(MapType $type, bool $nullable): string
    {
        $reflection = new ReflectionFunction('\\Flow\\Types\\DSL\\type_map');

        return sprintf(
            $nullable
                ? '\\Flow\\Types\\DSL\\type_optional(\%s(key_type: %s, value_type: %s))'
                : '\%s(key_type: %s, value_type: %s)',
            $reflection->getName(),
            $this->format($type->key()),
            $this->format($type->value()),
        );
    }

    /**
     * @param Type<mixed> $type
     */
    private function formatSimpleType(Type $type, bool $nullable): string
    {
        $reflection = match ($type::class) {
            ArrayType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_array'),
            StringType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_string'),
            IntegerType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_integer'),
            BooleanType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_boolean'),
            FloatType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_float'),
            DateTimeType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_datetime'),
            DateType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_date'),
            TimeType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_time'),
            ResourceType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_resource'),
            NullType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_null'),
            UuidType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_uuid'),
            CallableType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_callable'),
            JsonType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_json'),
            HTMLType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_html'),
            HTMLElementType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_html_element'),
            XMLType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_xml'),
            XMLElementType::class => new ReflectionFunction('\\Flow\\Types\\DSL\\type_xml_element'),
            default => throw new RuntimeException('Type ' . $type->toString() . ' is not a simple definition'),
        };

        if ($type instanceof NullType) {
            return sprintf('\%s()', $reflection->getName());
        }

        return sprintf($nullable ? '\\Flow\\Types\\DSL\\type_optional(\%s())' : '\%s()', $reflection->getName());
    }

    /**
     * @param StructureType<array<array-key, mixed>> $type
     */
    private function formatStructureType(StructureType $type, bool $nullable): string
    {
        $reflection = new ReflectionFunction('\\Flow\\Types\\DSL\\type_structure');

        $fields = [];

        foreach ($type->elements() as $name => $element) {
            $fields[] = sprintf('"%s" => %s', $name, $this->format($element));
        }

        $arguments = sprintf('elements: [%s]', implode(', ', $fields));

        if (count($type->optionalElements())) {
            $optionalFields = [];

            foreach ($type->optionalElements() as $name => $element) {
                $optionalFields[] = sprintf('"%s" => %s', $name, $this->format($element));
            }

            $arguments .= sprintf(', optional_elements: [%s]', implode(', ', $optionalFields));
        }

        if ($type->allowsExtra()) {
            $arguments .= ', allow_extra: true';
        }

        return sprintf(
            $nullable ? '\\Flow\\Types\\DSL\\type_optional(\%s(%s))' : '\%s(%s)',
            $reflection->getName(),
            $arguments,
        );
    }
}
