<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

final class MarketRowsMother
{
    /**
     * Five daily quotes whose Index is a native integer and whose remaining columns are strings,
     * so from_array() infers ?integer plus six ?string.
     *
     * @return array<int, array<string, int|string>>
     */
    public static function fiveDays(): array
    {
        return self::days(static fn(int $day): int => $day);
    }

    /**
     * fiveDays() with the Index carried as a string.
     *
     * @return array<int, array<string, int|string>>
     */
    public static function fiveDaysWithStringIndex(): array
    {
        return self::days(static fn(int $day): string => (string) $day);
    }

    /**
     * @param callable(int): (int|string) $index
     *
     * @return array<int, array<string, int|string>>
     */
    private static function days(callable $index): array
    {
        $rows = [];

        foreach ([19, 20, 21, 22, 23] as $position => $day) {
            $rows[] = [
                'Index' => $index($position + 1),
                'Date' => '2024-01-' . $day,
                'Close' => '2029.3',
                'Volume' => '166078.0',
                'Open' => '2027.4',
                'High' => '2041.9',
                'Low' => '2022.2',
            ];
        }

        return $rows;
    }
}
