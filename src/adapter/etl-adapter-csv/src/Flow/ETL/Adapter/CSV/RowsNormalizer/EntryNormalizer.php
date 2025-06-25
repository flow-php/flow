<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use function Flow\ETL\DSL\{date_interval_to_microseconds};
use function Flow\Types\DSL\type_json;
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
     */
    public function normalize(Entry $entry) : string|float|int|bool|null
    {
        $value = match ($entry::class) {
            UuidEntry::class,
            XMLElementEntry::class,
            XMLEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            EnumEntry::class => $entry->value()?->name,
            ListEntry::class,
            MapEntry::class,
            StructureEntry::class,
            JsonEntry::class => type_json()->cast($entry->value()),
            default => $entry->value(),
        };

        // Ensure we return only the expected types
        if (\is_string($value)) {
            return $value;
        }

        if (\is_float($value)) {
            return $value;
        }

        if (\is_int($value)) {
            return $value;
        }

        if (\is_bool($value)) {
            return $value;
        }

        if ($value === null) {
            return null;
        }

        // Handle remaining types
        if (\is_resource($value)) {
            return (string) $value;
        }

        if (\is_object($value) && \method_exists($value, '__toString')) {
            return $value->__toString();
        }

        if (\is_array($value) || \is_object($value)) {
            return '';
        }

        // At this point, we should have covered all cases
        return '';
    }
}
