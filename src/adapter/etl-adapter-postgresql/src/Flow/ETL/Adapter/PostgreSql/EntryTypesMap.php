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

use function array_key_exists;
use function array_merge;

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
 *     IntegerEntry::class => ValueType::INT2,
 *     ListEntry::class => ValueType::TEXT_ARRAY,
 * ]);
 * ```
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
        DateTimeEntry::class => ValueType::TIMESTAMPTZ,
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
     * @param array<class-string<Entry<mixed>>, ValueType> $overrides Entry class to ValueType mappings that override defaults
     */
    public function __construct(array $overrides = [])
    {
        $this->typeMap = array_merge(self::DEFAULT_TYPES, $overrides);
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
}
