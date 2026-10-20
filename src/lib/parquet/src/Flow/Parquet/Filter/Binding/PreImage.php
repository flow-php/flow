<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;

interface PreImage
{
    /**
     * Null when the column's decoded values cannot be compared with the literal exactly.
     */
    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange;
}
