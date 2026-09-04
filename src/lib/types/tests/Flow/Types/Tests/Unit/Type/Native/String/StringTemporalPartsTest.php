<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native\String;

use Flow\Types\Type\Native\String\StringTemporalParts;
use PHPUnit\Framework\Attributes\DataProvider;
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
        ];
    }

    #[DataProvider('inputs')]
    public function test_the_day_has_to_be_present_in_the_input(string $value, bool $explicit): void
    {
        static::assertSame($explicit, StringTemporalParts::hasExplicitDay($value));
    }

    public function test_a_month_precision_cell_is_neither_a_date_nor_a_datetime(): void
    {
        $parts = StringTemporalParts::from('2024-01');

        static::assertFalse($parts->isDate());
        static::assertFalse($parts->isDateTime());
    }

    public function test_a_calendar_date_is_a_date_and_not_a_datetime(): void
    {
        $parts = StringTemporalParts::from('2024-01-01');

        static::assertTrue($parts->isDate());
        static::assertFalse($parts->isDateTime());
    }

    public function test_a_date_carrying_a_time_is_a_datetime_and_not_a_date(): void
    {
        $parts = StringTemporalParts::from('2024-01-01 10:00');

        static::assertTrue($parts->isDateTime());
        static::assertFalse($parts->isDate());
    }

    public function test_a_relative_time_counts_as_a_time(): void
    {
        static::assertTrue(StringTemporalParts::from('2023-01-01 +10 hours')->isDateTime());
    }

    public function test_an_unparseable_value_is_neither(): void
    {
        foreach (['not a date', '2021-13-01', ''] as $value) {
            $parts = StringTemporalParts::from($value);

            static::assertFalse($parts->isDate(), $value);
            static::assertFalse($parts->isDateTime(), $value);
        }
    }
}
