<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function Flow\ETL\DSL\type_json;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row\Entry;

final class EntryNormalizer
{
    public function __construct(
        private readonly Caster $caster,
        private readonly string $dateTimeFormat = \DateTimeInterface::ATOM,
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
            Entry\EnumEntry::class => $entry->value()?->name,
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class,
            Entry\JsonEntry::class => $this->caster->to(type_json())->value($entry->value()),
            default => $entry->value(),
        };
    }
}
