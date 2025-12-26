<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\{BooleanEntry,
    DateEntry,
    DateTimeEntry,
    EnumEntry,
    FloatEntry,
    HTMLElementEntry,
    HTMLEntry,
    IntegerEntry,
    JsonEntry,
    ListEntry,
    MapEntry,
    StringEntry,
    StructureEntry,
    TimeEntry,
    UuidEntry,
    XMLElementEntry,
    XMLEntry};

final readonly class EntryNormalizer
{
    public function __construct(
        private string $dateTimeFormat = \DateTimeInterface::ATOM,
        private string $dateFormat = 'Y-m-d',
    ) {
    }

    /**
     * @param Entry<mixed> $entry
     */
    public function normalize(Entry $entry) : string|float|int|bool|null
    {
        return match ($entry::class) {
            BooleanEntry::class,
            IntegerEntry::class,
            FloatEntry::class,
            StringEntry::class => $entry->value(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            JsonEntry::class,
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class => $this->normalizeToJson($entry->value()),
            EnumEntry::class,
            UuidEntry::class,
            XMLEntry::class,
            XMLElementEntry::class,
            HTMLEntry::class,
            HTMLElementEntry::class => $entry->toString(),
            default => throw new InvalidArgumentException('Unknown entry type: ' . $entry::class),
        };
    }

    private function normalizeEnumEntry(EnumEntry $entry) : ?string
    {
        $value = $entry->value();

        if ($value instanceof \BackedEnum) {
            return (string) $value->value;
        }

        return $value?->name;
    }

    private function normalizeToJson(mixed $value) : ?string
    {
        return $value !== null ? \json_encode($value, JSON_THROW_ON_ERROR) : null;
    }
}
