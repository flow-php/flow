<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use DateInterval;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\date_interval_to_microseconds;
use function Flow\ETL\DSL\date_interval_to_milliseconds;
use function Flow\ETL\DSL\date_interval_to_seconds;

final class DateIntervalFunctionsTest extends FlowTestCase
{
    public static function date_interval_provider(): Generator
    {
        // Existing test cases
        yield [
            'interval' => new DateInterval('P1D'),
            'seconds' => 86400,
            'milliseconds' => 86400000,
            'microseconds' => 86400000000,
        ];
        yield [
            'interval' => new DateInterval('PT1H'),
            'seconds' => 3600,
            'milliseconds' => 3600000,
            'microseconds' => 3600000000,
        ];
        yield [
            'interval' => new DateInterval('PT1M'),
            'seconds' => 60,
            'milliseconds' => 60000,
            'microseconds' => 60000000,
        ];
        yield [
            'interval' => new DateInterval('PT1S'),
            'seconds' => 1,
            'milliseconds' => 1000,
            'microseconds' => 1000000,
        ];
        yield [
            'interval' => new DateInterval('P1DT1H1M1S'),
            'seconds' => 90061,
            'milliseconds' => 90061000,
            'microseconds' => 90061000000,
        ];
        yield [
            'interval' => new DateInterval('P1DT1H1M1S'),
            'seconds' => 90061,
            'milliseconds' => 90061000,
            'microseconds' => 90061000000,
        ];

        // Edge cases
        // Fractional seconds are ignored in conversion to seconds
        $fractionalSeconds = new DateInterval('PT1S');
        // @mago-ignore analysis:invalid-property-write
        $fractionalSeconds->f = 0.4;
        yield ['interval' => $fractionalSeconds, 'seconds' => 2, 'milliseconds' => 1400, 'microseconds' => 1400000];

        // Inverted interval
        $invertedInterval = new DateInterval('P1D');
        // @mago-ignore analysis:invalid-property-write
        $invertedInterval->invert = 1;
        yield [
            'interval' => $invertedInterval,
            'seconds' => -86400,
            'milliseconds' => -86400000,
            'microseconds' => -86400000000,
        ];

        // Zero values
        yield ['interval' => new DateInterval('PT0S'), 'seconds' => 0, 'milliseconds' => 0, 'microseconds' => 0];

        // Large values
        yield [
            'interval' => new DateInterval('P1000D'),
            'seconds' => 86400000,
            'milliseconds' => 86400000000,
            'microseconds' => 86400000000000,
        ];
    }

    #[TestWith([
        new DateInterval('P1Y'),
        "Relative DateInterval (with months/years) can't be converted to microseconds.",
    ])]
    #[TestWith([
        new DateInterval('P1M'),
        "Relative DateInterval (with months/years) can't be converted to microseconds.",
    ])]
    public function test_converting_relative_date_intervals_to_microseconds(
        DateInterval $interval,
        string $exceptionMessage,
    ): void {
        $this->expectExceptionMessage($exceptionMessage);
        date_interval_to_microseconds($interval);
    }

    #[TestWith([
        new DateInterval('P1Y'),
        "Relative DateInterval (with months/years) can't be converted to milliseconds.",
    ])]
    #[TestWith([
        new DateInterval('P1M'),
        "Relative DateInterval (with months/years) can't be converted to milliseconds.",
    ])]
    public function test_converting_relative_date_intervals_to_milliseconds(
        DateInterval $interval,
        string $exceptionMessage,
    ): void {
        $this->expectExceptionMessage($exceptionMessage);
        date_interval_to_milliseconds($interval);
    }

    #[TestWith([new DateInterval('P1Y'), "Relative DateInterval (with months/years) can't be converted to seconds."])]
    #[TestWith([new DateInterval('P1M'), "Relative DateInterval (with months/years) can't be converted to seconds."])]
    public function test_converting_relative_date_intervals_to_seconds(
        DateInterval $interval,
        string $exceptionMessage,
    ): void {
        $this->expectExceptionMessage($exceptionMessage);
        date_interval_to_seconds($interval);
    }

    #[DataProvider('date_interval_provider')]
    public function test_date_interval_to_milliseconds(
        DateInterval $interval,
        int $seconds,
        int $milliseconds,
        int $microseconds,
    ): void {
        static::assertEquals($seconds, date_interval_to_seconds($interval));
        static::assertEquals($milliseconds, date_interval_to_milliseconds($interval));
        static::assertEquals($microseconds, date_interval_to_microseconds($interval));
    }
}
