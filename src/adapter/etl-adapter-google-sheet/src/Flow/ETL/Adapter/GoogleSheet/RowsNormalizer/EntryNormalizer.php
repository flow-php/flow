<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\GoogleSheet\RowsNormalizer;

use function Flow\Types\DSL\type_json;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{DateEntry, DateTimeEntry, EnumEntry, JsonEntry, ListEntry, MapEntry, StructureEntry, TimeEntry, UuidEntry, XMLElementEntry, XMLEntry};

final readonly class EntryNormalizer
{
    public function __construct(
        private string $dateTimeFormat,
        private string $dateFormat,
    ) {
    }

    /**
     * @param Entry<mixed, mixed> $entry
     */
    public function normalize(Entry $entry) : string|float|int|bool|null
    {
        return match ($entry::class) {
            UuidEntry::class,
            XMLElementEntry::class,
            XMLEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value() === null ? '' : $entry->value()->format($this->dateTimeFormat),
            DateEntry::class => $entry->value() === null ? '' : $entry->value()->format($this->dateFormat),
            TimeEntry::class => $entry->toString(),
            EnumEntry::class => $entry->value()?->name,
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class,
            JsonEntry::class => type_json()->cast($entry->value()),
            default => $entry->value() ?? '',
        };
    }
}
