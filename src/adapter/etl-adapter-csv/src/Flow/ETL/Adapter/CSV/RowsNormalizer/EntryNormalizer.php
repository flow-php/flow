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
use function json_encode;

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
        return match ($entry::class) {
            UuidEntry::class, XMLElementEntry::class, XMLEntry::class, JsonEntry::class => $entry->toString(),
            DateTimeEntry::class => $entry->value()?->format($this->dateTimeFormat),
            DateEntry::class => $entry->value()?->format($this->dateFormat),
            TimeEntry::class => ($timeValue = $entry->value()) !== null
                ? date_interval_to_microseconds($timeValue)
                : null,
            EnumEntry::class => $entry->value()?->name,
            ListEntry::class, MapEntry::class, StructureEntry::class => json_encode(
                $entry->value(),
                JSON_THROW_ON_ERROR,
            ),
            default => ScalarCast::fromMixed($entry->value()),
        };
    }
}
