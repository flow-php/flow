<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Column\Physical;

use function intdiv;

/**
 * Howard Hinnant's days_from_civil: 0 is 1970-01-01, negative before.
 */
final readonly class DaysFromCivil
{
    public function of(int $year, int $month, int $day): int
    {
        $year -= $month <= 2 ? 1 : 0;
        $era = intdiv($year >= 0 ? $year : $year - 399, 400);
        $yearOfEra = $year - ($era * 400);

        return (
            ($era * 146_097) + (
                ($yearOfEra * 365) + intdiv($yearOfEra, 4) - intdiv($yearOfEra, 100)
                + (intdiv((153 * ($month > 2 ? $month - 3 : $month + 9)) + 2, 5) + $day - 1)
            )
            - 719_468
        );
    }
}
