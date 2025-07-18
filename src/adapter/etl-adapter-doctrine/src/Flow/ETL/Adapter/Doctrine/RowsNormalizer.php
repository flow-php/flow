<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Row\Entry\{XMLElementEntry, XMLEntry};
use Flow\ETL\Rows;

final class RowsNormalizer
{
    /**
     * Normalize row data to ensure XML entries are converted to strings.
     *
     * @return array<int, array<string, mixed>>
     */
    public function normalize(Rows $rows) : array
    {
        $normalizedData = [];

        foreach ($rows as $row) {
            $normalizedRow = [];

            foreach ($row->entries() as $entry) {
                if ($entry instanceof XMLEntry || $entry instanceof XMLElementEntry) {
                    $normalizedRow[$entry->name()] = $entry->toString();
                } else {
                    $normalizedRow[$entry->name()] = $entry->value();
                }
            }

            $normalizedData[] = $normalizedRow;
        }

        return $normalizedData;
    }
}
