<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Fixtures\CallUserFunc;

use function count;

final class StaticCalculator
{
    /**
     * @param array<mixed> $array
     */
    public static function count(array $array): int
    {
        return count($array);
    }
}
