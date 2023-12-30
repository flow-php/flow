<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\XML;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\Entry\{ArrayEntry, ListEntry, MapEntry, ObjectEntry, StructureEntry};
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

            /**
             * @psalm-suppress InvalidCast
             * @psalm-suppress PossiblyNullArgument
             */
            foreach ($row->entries() as $entry) {
                $columns[$entry->name()] = match ($entry::class) {
                    ArrayEntry::class,
                    ListEntry::class,
                    MapEntry::class,
                    StructureEntry::class => throw new RuntimeException('Entry of type ' . $entry::class . ' cannot be normalized to XML values.'),
                    ObjectEntry::class => match ($entry->value() instanceof \Stringable) {
                        /** @phpstan-ignore-next-line */
                        false => throw new RuntimeException('Entry of type ' . \get_class($entry->value()) . ' cannot be normalized to XML values.'),
                        true => (string) $entry->value(),
                    },
                    default => $entry->toString(),
                };
            }

            yield $columns;
        }
    }
}
