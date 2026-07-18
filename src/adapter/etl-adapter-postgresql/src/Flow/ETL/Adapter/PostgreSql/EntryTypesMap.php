<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
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
     * Default mapping of Flow type classes to PostgreSQL value types.
     *
     * @var array<class-string<Type<mixed>>, ValueType>
     */
    public const array DEFAULT_TYPES = [
        StringType::class => ValueType::TEXT,
        IntegerType::class => ValueType::INT8,
        FloatType::class => ValueType::FLOAT8,
        BooleanType::class => ValueType::BOOL,
        DateType::class => ValueType::DATE,
        DateTimeType::class => ValueType::TIMESTAMP,
        TimeType::class => ValueType::TIME,
        UuidType::class => ValueType::UUID,
        JsonType::class => ValueType::JSONB,
        XMLType::class => ValueType::XML,
        LogicalXMLElementType::class => ValueType::XML,
        HTMLType::class => ValueType::TEXT,
        HTMLElementType::class => ValueType::TEXT,
        EnumType::class => ValueType::TEXT,
        ListType::class => ValueType::JSONB,
        MapType::class => ValueType::JSONB,
        StructureType::class => ValueType::JSONB,
    ];

    /**
     * @var array<class-string<Type<mixed>>, ValueType>
     */
    private array $typeMap;

    /**
     * @var array<class-string<Type<mixed>>, ColumnType>
     */
    private array $columnTypeMap;

    /**
     * @param array<class-string<Type<mixed>>, ValueType> $overrides Flow type class to ValueType mappings that override defaults
     * @param array<class-string<Type<mixed>>, ColumnType> $columnTypeOverrides Flow Type class to ColumnType mappings that override defaults
     */
    public function __construct(array $overrides = [], array $columnTypeOverrides = [])
    {
        $this->typeMap = array_merge(self::DEFAULT_TYPES, $overrides);
        $this->columnTypeMap = array_merge(self::defaultColumnTypes(), $columnTypeOverrides);
    }

    /**
     * Maps a column value + Flow type to a TypedValue suitable for PostgreSQL queries.
     *
     * @param Type<mixed> $type
     *
     * @throws TypeMappingException when the Flow type is not in the map
     */
    public function map(string $column, Type $type, mixed $value): ?TypedValue
    {
        if ($value === null) {
            return null;
        }

        if (!array_key_exists($type::class, $this->typeMap)) {
            throw TypeMappingException::unmappedColumn($column, $type::class);
        }

        return new TypedValue($value, $this->typeMap[$type::class]);
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
