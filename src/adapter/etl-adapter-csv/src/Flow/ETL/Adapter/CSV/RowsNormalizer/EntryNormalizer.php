<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function Flow\ETL\DSL\{date_interval_to_microseconds};
use function Flow\Types\DSL\type_json;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{DateEntry, DateTimeEntry, EnumEntry, JsonEntry, ListEntry, MapEntry, StructureEntry, TimeEntry, UuidEntry, XMLElementEntry, XMLEntry};

final readonly class EntryNormalizer
{
    public function __construct(
        private string $dateTimeFormat = \DateTimeInterface::ATOM,
        private string $dateFormat = 'Y-m-d',
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
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            EnumEntry::class => $entry->value()?->name,
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class,
            JsonEntry::class => type_json()->cast($entry->value()),
            default => $entry->value(),
        };
    }
}
