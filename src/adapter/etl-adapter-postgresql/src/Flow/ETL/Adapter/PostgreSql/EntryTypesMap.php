<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{BooleanEntry, DateEntry, DateTimeEntry, EnumEntry, FloatEntry, HTMLElementEntry, HTMLEntry, IntegerEntry, JsonEntry, ListEntry, MapEntry, StringEntry, StructureEntry, TimeEntry, UuidEntry, XMLElementEntry, XMLEntry};
use Flow\PostgreSql\Client\TypedValue;
use Flow\PostgreSql\Client\Types\PostgreSqlType;

/**
 * Maps ETL Entry types to PostgreSQL types.
 *
 * Users can customize the mapping by passing overrides to the constructor.
 * Entry classes not in the map will throw TypeMappingException.
 *
 * Example usage:
 * ```php
 * // Use defaults
 * $map = new EntryTypesMap();
 *
 * // Override specific types
 * $map = new EntryTypesMap([
 *     IntegerEntry::class => PostgreSqlType::INT2,
 *     ListEntry::class => PostgreSqlType::TEXT_ARRAY,
 * ]);
 * ```
 */
final readonly class EntryTypesMap
{
    /**
     * Default mapping of Entry classes to PostgreSQL types.
     *
     * @var array<class-string<Entry<mixed>>, PostgreSqlType>
     */
    public const array DEFAULT_TYPES = [
        StringEntry::class => PostgreSqlType::TEXT,
        IntegerEntry::class => PostgreSqlType::INT8,
        FloatEntry::class => PostgreSqlType::FLOAT8,
        BooleanEntry::class => PostgreSqlType::BOOL,
        DateEntry::class => PostgreSqlType::DATE,
        DateTimeEntry::class => PostgreSqlType::TIMESTAMPTZ,
        TimeEntry::class => PostgreSqlType::TIME,
        UuidEntry::class => PostgreSqlType::UUID,
        JsonEntry::class => PostgreSqlType::JSONB,
        XMLEntry::class => PostgreSqlType::XML,
        XMLElementEntry::class => PostgreSqlType::XML,
        HTMLEntry::class => PostgreSqlType::TEXT,
        HTMLElementEntry::class => PostgreSqlType::TEXT,
        EnumEntry::class => PostgreSqlType::TEXT,
        ListEntry::class => PostgreSqlType::JSONB,
        MapEntry::class => PostgreSqlType::JSONB,
        StructureEntry::class => PostgreSqlType::JSONB,
    ];

    /**
     * @var array<class-string<Entry<mixed>>, PostgreSqlType>
     */
    private array $typeMap;

    /**
     * @param array<class-string<Entry<mixed>>, PostgreSqlType> $overrides Entry class to PostgreSqlType mappings that override defaults
     */
    public function __construct(array $overrides = [])
    {
        $this->typeMap = \array_merge(self::DEFAULT_TYPES, $overrides);
    }

    /**
     * Maps an Entry to a TypedValue suitable for PostgreSQL queries.
     *
     * @param Entry<mixed> $entry
     *
     * @throws TypeMappingException when entry type is not in the map
     */
    public function mapEntry(Entry $entry) : ?TypedValue
    {
        if ($entry->value() === null) {
            return null;
        }

        $entryClass = $entry::class;

        if (!\array_key_exists($entryClass, $this->typeMap)) {
            throw TypeMappingException::ambiguousEntryType($entryClass);
        }

        return new TypedValue($entry->value(), $this->typeMap[$entryClass]);
    }
}
