<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal;

use Flow\ETL\Adapter\Seal\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Rows;
use Generator;

final readonly class RowsNormalizer
{
    public function __construct(
        private EntryNormalizer $normalizer,
    ) {}

    /**
     * @return Generator<int, array<string, mixed>>
     */
    public function normalize(Rows $rows): Generator
    {
        foreach ($rows as $row) {
            $document = [];

            foreach ($row->entries() as $entry) {
                $document[$entry->name()] = $this->normalizer->normalize($entry);
            }

            yield $document;
        }
    }
}
