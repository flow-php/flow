<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\HTMLElementEntry;
use Flow\ETL\Row\Entry\HTMLEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\TimeEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\ValueType;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType as LogicalXMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;

use function array_key_exists;
use function array_merge;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml;

/**
 * Maps between Flow ETL types and PostgreSQL types.
 */
final readonly class EntryTypesMap
{
    /**
     * Default mapping of Entry classes to PostgreSQL types.
     *
     * @var array<class-string<Entry<mixed>>, ValueType>
     */
    public const array DEFAULT_TYPES = [
        StringEntry::class => ValueType::TEXT,
        IntegerEntry::class => ValueType::INT8,
        FloatEntry::class => ValueType::FLOAT8,
        BooleanEntry::class => ValueType::BOOL,
        DateEntry::class => ValueType::DATE,
        DateTimeEntry::class => ValueType::TIMESTAMP,
        TimeEntry::class => ValueType::TIME,
        UuidEntry::class => ValueType::UUID,
        JsonEntry::class => ValueType::JSONB,
        XMLEntry::class => ValueType::XML,
        XMLElementEntry::class => ValueType::XML,
        HTMLEntry::class => ValueType::TEXT,
        HTMLElementEntry::class => ValueType::TEXT,
        EnumEntry::class => ValueType::TEXT,
        ListEntry::class => ValueType::JSONB,
        MapEntry::class => ValueType::JSONB,
        StructureEntry::class => ValueType::JSONB,
    ];

    /**
     * @var array<class-string<Entry<mixed>>, ValueType>
     */
    private array $typeMap;

    /**
     * @var array<class-string<Type<mixed>>, ColumnType>
     */
    private array $columnTypeMap;

    /**
     * @param array<class-string<Entry<mixed>>, ValueType> $overrides Entry class to ValueType mappings that override defaults
     * @param array<class-string<Type<mixed>>, ColumnType> $columnTypeOverrides Flow Type class to ColumnType mappings that override defaults
     */
    public function __construct(array $overrides = [], array $columnTypeOverrides = [])
    {
        $this->typeMap = array_merge(self::DEFAULT_TYPES, $overrides);
        $this->columnTypeMap = array_merge(self::defaultColumnTypes(), $columnTypeOverrides);
    }

    /**
     * Maps an Entry to a TypedValue suitable for PostgreSQL queries.
     *
     * @param Entry<mixed> $entry
     *
     * @throws TypeMappingException when entry type is not in the map
     */
    public function mapEntry(Entry $entry): ?TypedValue
    {
        if ($entry->value() === null) {
            return null;
        }

        $entryClass = $entry::class;

        if (!array_key_exists($entryClass, $this->typeMap)) {
            throw TypeMappingException::ambiguousEntryType($entryClass);
        }

        return new TypedValue($entry->value(), $this->typeMap[$entryClass]);
    }

    /**
     * Maps a Flow type to a PostgreSQL DDL column type.
     *
     * @param Type<mixed> $type
     *
     * @throws TypeMappingException when the Flow type cannot be mapped
     */
    public function toColumnType(Type $type): ColumnType
    {
        $typeClass = $type::class;

        if (!array_key_exists($typeClass, $this->columnTypeMap)) {
            throw TypeMappingException::unsupportedFlowType($typeClass);
        }

        return $this->columnTypeMap[$typeClass];
    }

    /**
     * Maps a PostgreSQL DDL column type back to a canonical Flow type.
     *
     * @throws TypeMappingException when the PostgreSQL type cannot be mapped
     *
     * @return Type<mixed>
     */
    public function toFlowType(ColumnType $columnType): Type
    {
        $name = $columnType->normalize()['name'];

        return match ($name) {
            'int2', 'int4', 'int8', 'smallserial', 'serial', 'bigserial' => type_integer(),
            'float4', 'float8', 'numeric' => type_float(),
            'bool' => type_boolean(),
            'text', 'varchar', 'bpchar', 'bytea' => type_string(),
            'date' => type_date(),
            'time' => type_time(),
            'timestamp', 'timestamptz' => type_datetime(),
            'uuid' => type_uuid(),
            'json', 'jsonb' => type_json(),
            'xml' => type_xml(),
            default => throw TypeMappingException::unsupportedColumnType($name),
        };
    }

    /**
     * @return array<class-string<Type<mixed>>, ColumnType>
     */
    private static function defaultColumnTypes(): array
    {
        return [
            IntegerType::class => ColumnType::bigint(),
            StringType::class => ColumnType::text(),
            FloatType::class => ColumnType::doublePrecision(),
            BooleanType::class => ColumnType::boolean(),
            DateType::class => ColumnType::date(),
            TimeType::class => ColumnType::time(),
            DateTimeType::class => ColumnType::timestamp(),
            UuidType::class => ColumnType::uuid(),
            JsonType::class => ColumnType::jsonb(),
            ListType::class => ColumnType::jsonb(),
            MapType::class => ColumnType::jsonb(),
            StructureType::class => ColumnType::jsonb(),
            EnumType::class => ColumnType::text(),
            XMLType::class => ColumnType::xml(),
            LogicalXMLElementType::class => ColumnType::xml(),
            HTMLType::class => ColumnType::text(),
            HTMLElementType::class => ColumnType::text(),
        ];
    }
}
