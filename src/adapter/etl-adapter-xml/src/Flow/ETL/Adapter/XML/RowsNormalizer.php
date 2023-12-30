<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Entry\MapEntry;
use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Rows;

final class RowsNormalizer
{
    /**
     * @return \Generator<mixed, array<string, string>>
     */
    public function normalize(Rows $rows) : \Generator
    {
        foreach ($rows as $row) {
            $columns = [];

            /** @psalm-suppress InvalidCast */
            foreach ($row->entries() as $entry) {
                $columns[$entry->name()] = match ($entry::class) {
                    ArrayEntry::class,
                    ListEntry::class,
                    MapEntry::class,
                    StructureEntry::class => throw new RuntimeException('Entry of type ' . $entry::class . ' cannot be normalized to XML values.'),
                    ObjectEntry::class => match ($entry->value() instanceof \Stringable) {
                        false => throw new RuntimeException('Entry of type ' . \get_class($entry->value()) . ' cannot be normalized to XML values.'),
                        /** @phpstan-ignore-next-line */
                        true => (string) $entry->value(),
                    },
                    default => $entry->toString(),
                };
            }

            yield $columns;
        }
    }
}
