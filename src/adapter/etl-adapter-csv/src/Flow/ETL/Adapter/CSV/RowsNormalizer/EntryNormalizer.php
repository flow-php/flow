<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\RowsNormalizer;

use DateTimeInterface;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Entry\DateEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\EnumEntry;
use Flow\ETL\Row\Entry\JsonEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Entry\TimeEntry;
use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Row\Entry\XMLElementEntry;
use Flow\ETL\Row\Entry\XMLEntry;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_resource;
use function is_string;
use function json_encode;
use function method_exists;

use const JSON_THROW_ON_ERROR;

final readonly class EntryNormalizer
{
    public function __construct(
        private string $dateTimeFormat = DateTimeInterface::ATOM,
        private string $dateFormat = 'Y-m-d',
    ) {}

    /**
     * @param Entry<mixed> $entry
     */
    public function normalize(Entry $entry): string|float|int|bool|null
    {
        $value = match ($entry::class) {
            UuidEntry::class, XMLElementEntry::class, XMLEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => $entry->value() ? date_interval_to_microseconds($entry->value()) : null,
            EnumEntry::class => $entry->value()?->name,
            ListEntry::class, MapEntry::class, StructureEntry::class => json_encode(
                $entry->value(),
                JSON_THROW_ON_ERROR,
            ),
            JsonEntry::class => $entry->toString(),
            default => $entry->value(),
        };

        // Ensure we return only the expected types
        if (is_string($value)) {
            return $value;
        }

        if (is_float($value)) {
            return $value;
        }

        if (is_int($value)) {
            return $value;
        }

        if (is_bool($value)) {
            return $value;
        }

        if ($value === null) {
            return null;
        }

        // Handle remaining types
        if (is_resource($value)) {
            return (string) $value;
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return $value->__toString();
        }

        if (is_array($value) || is_object($value)) {
            return '';
        }

        // At this point, we should have covered all cases
        return '';
    }
}
