<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Adapter\CSV\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Rows;

final readonly class RowsNormalizer
{
    public function __construct(private EntryNormalizer $entryNormalizer)
    {
    }

    /**
     * @return \Generator<array<null|bool|float|int|string>>
     */
    public function normalize(Rows $rows) : \Generator
    {
        foreach ($rows as $row) {
            $normalizedRow = [];

            foreach ($row->entries() as $entry) {
                $normalizedRow[] = $this->entryNormalizer->normalize($entry);
            }

            yield $normalizedRow;
        }
    }
}
