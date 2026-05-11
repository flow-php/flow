<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class NativePHPRandomValueGeneratorTest extends FlowTestCase
{
    public static function integers_provider(): \Generator
    {
        foreach (range(1, 10) as $i) {
            yield [$i];
        }
    }

    /**
     * @return array<string, array{int, int}>
     */
    public static function invalid_range_provider(): array
    {
        return [
            'min greater than max' => [2, 1],
        ];
    }

    /**
     * @return array<string, array{int, int}>
     */
    public static function valid_range_provider(): array
    {
        return [
            'min equal to max' => [1, 1],
            'min less than max' => [1, 2],
            'min less than zero' => [-1, 1],
            'max and min less than zero' => [-1, -1],
        ];
    }

    public function test_can_create_random_int_from_given_range(): void
    {
        static::assertSame(1, (new NativePHPRandomValueGenerator())->int(1, 1));
        static::assertThat(
            (new NativePHPRandomValueGenerator())->int(1, 2),
            static::logicalOr(static::equalTo(1), static::equalTo(2)),
        );
    }

    #[DataProvider('integers_provider')]
    public function test_can_create_random_string_with_given_length(int $expectedLength): void
    {
        static::assertSame($expectedLength, mb_strlen((new NativePHPRandomValueGenerator())->string($expectedLength)));
    }

    public function test_empty_string_on_length_below_1(): void
    {
        static::assertSame('', (new NativePHPRandomValueGenerator())->string(0));
        static::assertSame('', (new NativePHPRandomValueGenerator())->string(-1));
    }

    #[DataProvider('invalid_range_provider')]
    public function test_fail_on_invalid_range(int $min, int $max): void
    {
        self::expectException(\ValueError::class);
        (new NativePHPRandomValueGenerator())->int($min, $max);
    }

    #[DataProvider('valid_range_provider')]
    public function test_return_random_int_on_valid_range(int $min, int $max): void
    {
        static::assertThat(
            (new NativePHPRandomValueGenerator())->int($min, $max),
            static::logicalOr(static::greaterThanOrEqual($min), static::lessThanOrEqual($max)),
        );
    }
}
