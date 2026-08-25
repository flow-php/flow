<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Rows;
use Generator;

/**
 * Carries the pairing so KWayMerge can hold spill runs and merged runs in one ordered list while each resolves
 * through its own storage. A mismatched pair is constructible; the invariant is upheld at the mint sites in
 * MergeSortProcessor, the only class where both storages are in scope.
 *
 * @internal
 */
final readonly class BucketRun
{
    public function __construct(
        public string $id,
        public Buckets $buckets,
    ) {}

    public function remove(): void
    {
        $this->buckets->remove($this->id);
    }

    /**
     * @return Generator<Rows>
     */
    public function rows(): Generator
    {
        yield from $this->buckets->rows($this->id);
    }
}
