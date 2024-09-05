<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\RowsNormalizer;

use function Flow\ETL\DSL\type_array;
use Flow\ETL\PHP\Type\Caster;
use Flow\ETL\Row\Entry;

final class EntryNormalizer
{
    public function __construct(
        private readonly Caster $caster,
        private readonly string $dateTimeFormat = \DateTimeInterface::ATOM,
    ) {
    }

    public function normalize(Entry $entry) : string|float|int|bool|array|null
    {
        return match ($entry::class) {
            Entry\UuidEntry::class => $entry->toString(),
            Entry\DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            Entry\EnumEntry::class => $entry->value()?->name,
            Entry\ArrayEntry::class,
            Entry\ListEntry::class,
            Entry\MapEntry::class,
            Entry\StructureEntry::class,
            Entry\JsonEntry::class,
            Entry\ObjectEntry::class => $this->caster->to(type_array())->value($entry->value()),
            default => $entry->value(),
        };
    }
}
