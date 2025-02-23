<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\RowsNormalizer;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use Flow\ETL\Row\Entry;

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
    public function normalize(Entry $entry) : string|float|int|bool|array|null
    {
        return match ($entry::class) {
            Entry\UuidEntry::class => $entry->toString(),
            Entry\DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            Entry\DateEntry::class => $entry->value()?->format($this->dateFormat),
            Entry\TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            Entry\EnumEntry::class => $entry->value()?->name,
            Entry\JsonEntry::class => $entry->value(),
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class,
            Entry\XMLElementEntry::class => $entry->toString(),
            Entry\XMLEntry::class => $entry->toString(),
            default => $entry->value(),
        };
    }
}
