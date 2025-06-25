<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\RowsNormalizer;

use function Flow\ETL\DSL\date_interval_to_microseconds;
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
     * @param Entry<mixed> $entry
     *
     * @return null|array<string, mixed>|bool|float|int|string
     */
    public function normalize(Entry $entry) : string|float|int|bool|array|null
    {
        return match ($entry::class) {
            UuidEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            EnumEntry::class => $entry->value()?->name,
            JsonEntry::class => $this->normalizeJsonValue($entry->value()),
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class,
            XMLElementEntry::class => $entry->toString(),
            XMLEntry::class => $entry->toString(),
            default => $this->normalizeValue($entry->value()),
        };
    }

    /**
     * @return null|array<string, mixed>|bool|float|int|string
     */
    private function normalizeJsonValue(mixed $value) : string|float|int|bool|array|null
    {
        if (\is_array($value)) {
            /** @var array<string, mixed> $normalizedArray */
            $normalizedArray = [];

            foreach ($value as $key => $val) {
                $normalizedArray[\is_string($key) ? $key : (string) $key] = $val;
            }

            return $normalizedArray;
        }

        return $this->normalizeValue($value);
    }

    /**
     * @return null|array<string, mixed>|bool|float|int|string
     */
    private function normalizeValue(mixed $value) : string|float|int|bool|array|null
    {
        if (\is_string($value) || \is_float($value) || \is_int($value) || \is_bool($value) || $value === null) {
            return $value;
        }

        if (\is_array($value)) {
            /** @var array<string, mixed> $normalizedArray */
            $normalizedArray = [];

            foreach ($value as $key => $val) {
                $normalizedArray[\is_string($key) ? $key : (string) $key] = $val;
            }

            return $normalizedArray;
        }

        // Fallback for unexpected types - convert to string
        return '';
    }
}
