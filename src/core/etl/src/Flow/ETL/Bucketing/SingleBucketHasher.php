<?php

declare(strict_types=1);

namespace Flow\ETL\Bucketing;

use function array_fill;
use function count;

final readonly class SingleBucketHasher implements Hasher
{
    public function hash(array $values): array
    {
        return array_fill(0, count($values), '0');
    }
}
