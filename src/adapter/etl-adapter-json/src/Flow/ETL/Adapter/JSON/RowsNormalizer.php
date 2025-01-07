<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Adapter\JSON\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Rows;

final readonly class RowsNormalizer
{
    public function __construct(private EntryNormalizer $normalizer)
    {

    }

    /**
     * @return \Generator<array<string, null|array|bool|float|int|string>>
     */
    public function normalize(Rows $rows) : \Generator
    {
        foreach ($rows as $row) {
            $normalizedRow = [];

            foreach ($row->entries() as $entry) {
                $normalizedRow[$entry->name()] = $this->normalizer->normalize($entry);
            }

            yield $normalizedRow;
        }
    }
}
