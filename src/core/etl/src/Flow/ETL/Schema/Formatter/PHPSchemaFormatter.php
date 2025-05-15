<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Formatter;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Schema;
use Flow\ETL\Schema\{Definition, Metadata, SchemaFormatter};
use Flow\ETL\Schema\Formatter\PHPFormatter\{TypeFormatter};
use Flow\ETL\Schema\Formatter\PHPFormatter\ValueFormatter;
use Flow\Types\Type\Logical\{DateTimeType,
    DateType,
    JsonType,
    ListType,
    MapType,
    StructureType,
    TimeType,
    UuidType,
    XMLElementType,
    XMLType};
use Flow\Types\Type\Native\{BooleanType, EnumType, FloatType, IntegerType, StringType};

final readonly class PHPSchemaFormatter implements SchemaFormatter
{
    public function __construct(
        private ValueFormatter $valueFormatter = new ValueFormatter(),
        private TypeFormatter $typeFormatter = new TypeFormatter(),
    ) {
    }

    public function format(Schema $schema) : string
    {
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\schema");

        return \sprintf(
            "\%s(%s);",
            $reflection->getName(),
            $this->formatSchema($schema)
        );
    }

    private function enumType(Definition $definition) : string
    {
        /** @var EnumType<\UnitEnum> $type */
        $type = $definition->type();
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\enum_schema");

        return \sprintf(
            '\%s("%s", type: \%s::class, nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $type->class,
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }

    private function floatType(Definition $definition) : string
    {
        /** @var FloatType $type */
        $type = $definition->type();
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\float_schema");

        return \sprintf(
            '\%s("%s", nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }

    private function formatMetadata(Metadata $metadata) : string
    {
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\schema_metadata");

        if ($metadata->isEmpty()) {
            return \sprintf('\%s()', $reflection->getName());
        }

        return \sprintf(
            '\%s(%s)',
            $reflection->getName(),
            $this->valueFormatter->format($metadata->normalize())
        );
    }

    private function formatSchema(Schema $schema, int $level = 1) : string
    {
        if (!\count($schema->definitions())) {
            return '';
        }
        $indention = \str_repeat('    ', $level);

        $definitions = "\n";

        foreach ($schema->definitions() as $definition) {
            $definitions .= $indention . match ($definition->type()::class) {
                StringType::class,
                IntegerType::class,
                BooleanType::class,
                DateType::class,
                TimeType::class,
                JsonType::class,
                UuidType::class,
                XMLType::class,
                XMLElementType::class,
                DateTimeType::class => $this->simpleType($definition),
                FloatType::class => $this->floatType($definition),
                EnumType::class => $this->enumType($definition),
                ListType::class => $this->listType($definition),
                MapType::class => $this->mapType($definition),
                StructureType::class => $this->structureType($definition),
                default => throw new RuntimeException('Type ' . $definition->type()->toString() . ' is not supported'),
            };

            $definitions .= ",\n";
        }

        return $definitions;
    }

    private function listType(Definition $definition) : string
    {
        /** @var ListType<mixed> $type */
        $type = $definition->type();
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\list_schema");

        return \sprintf(
            '\%s("%s", type: %s, nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $this->typeFormatter->format($type),
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }

    private function mapType(Definition $definition) : string
    {
        /** @var MapType<array-key, mixed> $type */
        $type = $definition->type();
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\map_schema");

        return \sprintf(
            '\%s("%s", type: %s, nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $this->typeFormatter->format($type),
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }

    private function simpleType(Definition $definition) : string
    {
        $reflection = match ($definition->type()::class) {
            StringType::class => new \ReflectionFunction("\Flow\ETL\DSL\string_schema"),
            IntegerType::class => new \ReflectionFunction("\Flow\ETL\DSL\integer_schema"),
            BooleanType::class => new \ReflectionFunction("\Flow\ETL\DSL\bool_schema"),
            DateType::class => new \ReflectionFunction("\Flow\ETL\DSL\date_schema"),
            DateTimeType::class => new \ReflectionFunction("\Flow\ETL\DSL\datetime_schema"),
            TimeType::class => new \ReflectionFunction("\Flow\ETL\DSL\\time_schema"),
            JsonType::class => new \ReflectionFunction("\Flow\ETL\DSL\\json_schema"),
            UuidType::class => new \ReflectionFunction("\Flow\ETL\DSL\\uuid_schema"),
            XMLType::class => new \ReflectionFunction("\Flow\ETL\DSL\\xml_schema"),
            XMLElementType::class => new \ReflectionFunction("\Flow\ETL\DSL\\xml_element_schema"),
            default => throw new RuntimeException('Type ' . $definition->type()->toString() . ' is not a simple definition'),
        };

        return \sprintf(
            '\%s("%s", nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }

    private function structureType(Definition $definition) : string
    {
        /** @var StructureType<array> $type */
        $type = $definition->type();
        $reflection = new \ReflectionFunction("\Flow\ETL\DSL\\structure_schema");

        return \sprintf(
            '\%s("%s", type: %s, nullable: %s, metadata: %s)',
            $reflection->getName(),
            $definition->entry()->name(),
            $this->typeFormatter->format($type),
            $definition->isNullable() ? 'true' : 'false',
            $this->formatMetadata($definition->metadata())
        );
    }
}
