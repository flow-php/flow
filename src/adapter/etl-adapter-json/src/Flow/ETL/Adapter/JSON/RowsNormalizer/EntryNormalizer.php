<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\RowsNormalizer;

use Flow\ETL\Row\Entry;

final class EntryNormalizer
{
    public function __construct(
        private readonly string $dateTimeFormat = \DateTimeInterface::ATOM,
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
            Entry\EnumEntry::class => $entry->value()?->name,
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class,
            Entry\JsonEntry::class,
            Entry\XMLElementEntry::class => $entry->toString(),
            Entry\XMLEntry::class => $entry->toString(),
            default => $entry->value(),
        };
    }
}
