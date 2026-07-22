<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Bucketing\Hasher;

use function array_fill;
use function count;

final readonly class ConstantHasher implements Hasher
{
    public function __construct(
        private string $hash,
    ) {}

    public function hash(array $values): array
    {
        return $values === [] ? [] : array_fill(0, count($values), $this->hash);
    }
}
