<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Row\Entry\UuidEntry;
use Flow\ETL\Rows;

final class RowsNormalizer
{
    /**
     * @return array<mixed, array<string, mixed>>
     */
    public function normalize(Rows $rows) : array
    {
        $normalizedRows = [];

        foreach ($rows as $row) {
            $columns = [];

            foreach ($row->entries() as $entry) {
                $columns[$entry->name()] = match ($entry::class) {
                    UuidEntry::class => $entry->value()->toString(),
                    default => $entry->value(),
                };
            }

            $normalizedRows[] = $columns;
        }

        return $normalizedRows;
    }
}
