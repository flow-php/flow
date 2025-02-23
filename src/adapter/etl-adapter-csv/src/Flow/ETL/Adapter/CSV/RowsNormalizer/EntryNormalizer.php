<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function Flow\ETL\DSL\{date_interval_to_microseconds, type_json};
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row\Entry;

final readonly class EntryNormalizer
{
    public function __construct(
        private Caster $caster,
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
            Entry\UuidEntry::class,
            Entry\XMLElementEntry::class,
            Entry\XMLEntry::class => $entry->toString(),
            Entry\DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            Entry\DateEntry::class => $entry->value()?->format($this->dateFormat),
            Entry\TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            Entry\EnumEntry::class => $entry->value()?->name,
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class,
            Entry\JsonEntry::class => $this->caster->to(type_json())->value($entry->value()),
            default => $entry->value(),
        };
    }
}
