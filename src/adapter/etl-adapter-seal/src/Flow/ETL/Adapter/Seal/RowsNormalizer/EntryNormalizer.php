<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\RowsNormalizer;

use DateTimeInterface;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;

use function array_keys;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

final readonly class EntryNormalizer
{
    public function __construct(
        private string $dateTimeFormat = DateTimeInterface::ATOM,
        private string $dateFormat = 'Y-m-d',
    ) {}

    /**
     * @param Entry<mixed> $entry
     *
     * @return null|array<array-key, mixed>|bool|float|int|string
     */
    public function normalize(Entry $entry): string|float|int|bool|array|null
    {
        return match ($entry::class) {
            UuidEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            EnumEntry::class => $entry->value()?->name,
            XMLElementEntry::class, XMLEntry::class => $entry->toString(),
            JsonEntry::class => $this->normalizeArray($entry->value()?->toArray()),
            ListEntry::class, MapEntry::class, StructureEntry::class => $this->normalizeArray($entry->value()),
            default => $this->normalizeValue($entry->value()),
        };
    }

    /**
     * @return null|array<array-key, mixed>
     */
    private function normalizeArray(mixed $value): ?array
    {
        if (!is_array($value)) {
            return null;
        }

        $normalized = [];

        foreach (array_keys($value) as $key) {
            $normalized[$key] = $this->normalizeValue($value[$key]);
        }

        return $normalized;
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|string
     */
    private function normalizeValue(mixed $value): string|float|int|bool|array|null
    {
        if ($value instanceof DateTimeInterface) {
            return $value->format($this->dateTimeFormat);
        }

        if (is_array($value)) {
            return $this->normalizeArray($value);
        }

        if (is_string($value) || is_int($value) || is_float($value) || is_bool($value)) {
            return $value;
        }

        return null;
    }
}
