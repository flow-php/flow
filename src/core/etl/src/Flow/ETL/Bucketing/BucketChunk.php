<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Rows;

final readonly class BucketChunk
{
    /**
     * @param list<string> $hashes bucket-key hashes aligned 1:1 with $rows
     * @param list<array<string, mixed>> $values bucket-key values extracted once, aligned 1:1 with $rows
     *                                           (kept so stats never re-read the rows)
     */
    public function __construct(
        public string $bucketId,
        public Rows $rows,
        public array $hashes,
        public array $values,
    ) {}
}
