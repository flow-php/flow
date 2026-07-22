<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Bucketing\Hasher;

use function count;

final class CountingHasher implements Hasher
{
    private int $hashedRows = 0;

    public function __construct(
        private readonly Hasher $inner,
    ) {}

    public function hash(array $values): array
    {
        $this->hashedRows += count($values);

        return $this->inner->hash($values);
    }

    public function hashedRows(): int
    {
        return $this->hashedRows;
    }
}
