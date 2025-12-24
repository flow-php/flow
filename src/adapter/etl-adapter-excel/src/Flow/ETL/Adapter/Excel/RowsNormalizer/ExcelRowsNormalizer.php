<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\RowsNormalizer;

use Flow\ETL\Row;
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

final readonly class ExcelRowsNormalizer
{
    public function __construct(
        private string $dateFormat = 'Y-m-d',
        private string $dateTimeFormat = 'Y-m-d H:i:s',
        private string $timeFormat = 'H:i:s',
    ) {
    }

    /**
     * @return array<int, string>
     */
    public function headers(Row $row) : array
    {
        $headers = [];

        foreach ($row->entries() as $entry) {
            $headers[] = $entry->name();
        }

        return $headers;
    }

    /**
     * @return array<int, null|bool|float|int|string>
     */
    public function normalize(Row $row) : array
    {
        $values = [];

        foreach ($row->entries() as $entry) {
            $values[] = $this->normalizeEntry($entry);
        }

        return $values;
    }

    /**
     * @param Entry<mixed> $entry
     */
    private function normalizeEntry(Entry $entry) : bool|float|int|string|null
    {
        $value = match ($entry::class) {
            BooleanEntry::class,
            IntegerEntry::class,
            FloatEntry::class,
            StringEntry::class => $entry->value(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value()?->format($this->timeFormat),
            UuidEntry::class => $entry->toString(),
            EnumEntry::class => $this->normalizeEnumEntry($entry),
            JsonEntry::class,
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class => $this->normalizeToJson($entry->value()),
            XMLEntry::class,
            XMLElementEntry::class,
            HTMLEntry::class,
            HTMLElementEntry::class => $entry->toString(),
            default => $entry->toString(),
        };

        if ($value === null) {
            return null;
        }

        if (\is_bool($value)) {
            return $value;
        }

        if (\is_int($value)) {
            return $value;
        }

        if (\is_float($value)) {
            return $value;
        }

        if (\is_string($value)) {
            return $value;
        }

        return $entry->toString();
    }

    private function normalizeEnum(\UnitEnum $enum) : string
    {
        if ($enum instanceof \BackedEnum) {
            return (string) $enum->value;
        }

        return $enum->name;
    }

    private function normalizeEnumEntry(EnumEntry $entry) : ?string
    {
        $value = $entry->value();

        if ($value === null) {
            return null;
        }

        return $this->normalizeEnum($value);
    }

    private function normalizeToJson(mixed $value) : ?string
    {
        return $value !== null ? \json_encode($value, JSON_THROW_ON_ERROR) : null;
    }
}
