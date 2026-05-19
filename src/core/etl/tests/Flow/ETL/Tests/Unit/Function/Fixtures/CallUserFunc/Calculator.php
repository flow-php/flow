<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc;

use function count;

final class Calculator
{
    /**
     * @param array<array-key, mixed> $array
     */
    public function count(array $array): int
    {
        return count($array);
    }
}
