<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\RowsNormalizer;

use BackedEnum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
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

use function json_encode;

final readonly class ExcelRowsNormalizer
{
    public function __construct(
        private string $dateFormat = 'Y-m-d',
        private string $dateTimeFormat = 'Y-m-d H:i:s',
        private string $timeFormat = 'H:i:s',
    ) {}

    /**
     * @return array<int, string>
     */
    public function headers(Row $row): array
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
    public function normalize(Row $row): array
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
    private function normalizeEntry(Entry $entry): bool|float|int|string|null
    {
        return match ($entry::class) {
            BooleanEntry::class, IntegerEntry::class, FloatEntry::class, StringEntry::class => $entry->value(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value()?->format($this->timeFormat),
            EnumEntry::class => $this->normalizeEnumEntry($entry),
            JsonEntry::class, ListEntry::class, MapEntry::class, StructureEntry::class => $this->normalizeToJson(
                $entry->value(),
            ),
            UuidEntry::class,
            XMLEntry::class,
            XMLElementEntry::class,
            HTMLEntry::class,
            HTMLElementEntry::class,
                => $entry->toString(),
            default => throw new InvalidArgumentException('Unknown entry type: ' . $entry::class),
        };
    }

    private function normalizeEnumEntry(EnumEntry $entry): ?string
    {
        $value = $entry->value();

        if ($value instanceof BackedEnum) {
            return (string) $value->value;
        }

        return $value?->name;
    }

    private function normalizeToJson(mixed $value): ?string
    {
        return $value !== null ? json_encode($value, JSON_THROW_ON_ERROR) : null;
    }
}
