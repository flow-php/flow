<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use Flow\ETL\Column\Physical\DaysFromCivil;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DaysFromCivilTest extends TestCase
{
    #[TestWith([1970, 1, 1, 0])]
    #[TestWith([1969, 12, 31, -1])]
    #[TestWith([1970, 1, 2, 1])]
    #[TestWith([2000, 2, 29, 11_016])]
    #[TestWith([2000, 3, 1, 11_017])]
    #[TestWith([1900, 3, 1, -25_508])]
    #[TestWith([2024, 12, 31, 20_088])]
    #[TestWith([-1, 12, 31, -719_529])]
    #[TestWith([0, 1, 1, -719_528])]
    public function test_counts_days_from_the_epoch(int $year, int $month, int $day, int $days): void
    {
        static::assertSame($days, (new DaysFromCivil())->of($year, $month, $day));
    }
}
