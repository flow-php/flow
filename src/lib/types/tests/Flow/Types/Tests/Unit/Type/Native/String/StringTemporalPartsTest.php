<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native\String;

use Flow\Types\Type\Native\String\StringTemporalParts;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class StringTemporalPartsTest extends TestCase
{
    /**
     * @return array<string, array{string, bool}>
     */
    public static function inputs(): array
    {
        return [
            'three numeric groups' => ['2024-01-01', true],
            'slashed' => ['12/31/2024', true],
            'spelled-out month' => ['Thursday, 02-Jun-2022', true],
            'month precision' => ['2024-01', false],
            'year only' => ['2024', false],
            'compact ISO' => ['20240305', true],
            'eight digits that are not a date' => ['12345678', true],
            'nine digits' => ['202403050', false],
        ];
    }

    /**
     * @return array<string, array{string, bool, bool}>
     */
    public static function temporal(): array
    {
        return [
            'ISO minutes, T' => ['2026-01-02T03:04', false, true],
            'ISO minutes, space' => ['2026-01-02 03:04', false, true],
            'ISO seconds' => ['2026-01-02T03:04:05', false, true],
            'ISO one fraction digit' => ['2026-01-02T03:04:05.1', false, true],
            'ISO nine fraction digits' => ['2026-01-02T03:04:05.123456789', false, true],
            'ISO ten fraction digits' => ['2026-01-02T03:04:05.1234567890', false, true],
            'ISO Z' => ['2026-01-02T03:04:05Z', false, true],
            'ISO +hh' => ['2026-01-02T03:04:05+02', false, true],
            'ISO bare zone hour beyond 24' => ['2026-01-02T03:04+99', false, true],
            'ISO -hhmm' => ['2026-01-02T03:04:05-0230', false, true],
            'ISO +hh:mm' => ['2026-01-02T03:04:05.5+02:00', false, true],
            'ISO zone at its upper bound' => ['2026-01-02T03:04+24:59', false, true],
            'ISO trailing newline' => ["2026-01-02T03:04:05Z\n", false, true],
            'ISO leap day' => ['2024-02-29T00:00', false, true],
            'ISO hour 24' => ['2026-01-02T24:00', false, true],
            'ISO second 60' => ['2026-01-02T03:04:60', false, true],
            'ISO hour 25' => ['2026-01-02T25:00', false, false],
            'ISO minute 60' => ['2026-01-02T03:60', false, false],
            'ISO second 61' => ['2026-01-02T03:04:61', false, false],
            'ISO zone hour 25' => ['2026-01-02T03:04+25:00', false, false],
            'ISO zone minute 60' => ['2026-01-02T03:04+0060', false, false],
            'ISO datetime on February 30' => ['2026-02-30T03:04', false, false],
            'ISO datetime in month 13' => ['2026-13-01T03:04', false, false],
            'ISO date' => ['2026-01-02', true, false],
            'ISO date, trailing newline' => ["2026-01-02\n", true, false],
            'ISO date, leap day' => ['2024-02-29', true, false],
            'ISO date, February 30' => ['2026-02-30', false, false],
            'ISO date, month 13' => ['2026-13-01', false, false],
            'ISO date, year zero' => ['0000-01-01', false, false],
            'month precision' => ['2024-01', false, false],
            'spelled-out month' => ['March 5, 2024', true, false],
            'slashed' => ['05/03/2024', true, false],
            'day-month-year' => ['02-Jun-2022', true, false],
            'compact ISO' => ['20240305', true, false],
            'eight digits that are not a date' => ['12345678', false, false],
            'relative time word' => ['2024-03-05 noon', false, true],
            'relative day word' => ['tomorrow 2024-03-05', false, true],
            'relative time' => ['2023-01-01 +10 hours', false, true],
            'wall clock' => ['now', false, false],
            'words' => ['not a date', false, false],
            'slashed month 13' => ['2021/13/01', false, false],
            'empty' => ['', false, false],
        ];
    }

    #[DataProvider('temporal')]
    public function test_date_and_datetime_verdicts(string $value, bool $date, bool $dateTime): void
    {
        $parts = StringTemporalParts::from($value);

        static::assertSame($date, $parts->isDate());
        static::assertSame($dateTime, $parts->isDateTime());
    }

    #[TestWith(['2026-01-02T03:04', true])]
    #[TestWith(['2026-01-02 03:04:05.123456789+02:00', true])]
    #[TestWith(["2026-01-02T03:04:05Z\n", true])]
    #[TestWith(['2026-02-30T03:04', false])]
    #[TestWith(['2026-01-02T25:00', false])]
    #[TestWith(['2026-01-02', false])]
    public function test_iso_date_time_gate(string $value, bool $expected): void
    {
        static::assertSame($expected, StringTemporalParts::isoDateTime($value));
    }

    #[TestWith(['2026-01-02', true])]
    #[TestWith(["2026-01-02\n", true])]
    #[TestWith(['2024-02-29', true])]
    #[TestWith(['2026-02-30', false])]
    #[TestWith(['0000-01-01', false])]
    #[TestWith(['2026-01-02T03:04', false])]
    public function test_iso_date_gate(string $value, bool $expected): void
    {
        static::assertSame($expected, StringTemporalParts::isoDate($value));
    }

    #[DataProvider('inputs')]
    public function test_the_day_has_to_be_present_in_the_input(string $value, bool $explicit): void
    {
        static::assertSame($explicit, StringTemporalParts::hasExplicitDay($value));
    }
}
