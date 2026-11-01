<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\Bucketing;

use Flow\ETL\Values\Rows;
use Generator;

/**
 * A mismatched pair is constructible; ExternalSort's mint sites keep it consistent.
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
