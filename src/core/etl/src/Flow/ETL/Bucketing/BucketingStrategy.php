<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Generator;

interface BucketingStrategy
{
    public function by(): References;

    /**
     * @param Generator<Rows> $rows
     *
     * @return Generator<BucketChunk>
     */
    public function bucketize(Generator $rows): Generator;

    public function sortedBy(): ?References;
}
