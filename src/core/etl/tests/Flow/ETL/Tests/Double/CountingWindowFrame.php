<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Rows;
use Flow\ETL\Window\WindowFrame;

final class CountingWindowFrame implements WindowFrame
{
    public int $calls = 0;

    /**
     * @param array{0: int, 1: int} $bounds
     */
    public function __construct(
        private readonly array $bounds = [0, 0],
    ) {}

    public function bounds(int $index, Rows $partition): array
    {
        $this->calls++;

        return $this->bounds;
    }
}
