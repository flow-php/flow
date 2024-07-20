<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\UniqueFactory;
use PHPUnit\Framework\TestCase;

final class UniqueFactoryTest extends TestCase
{
    public static function integers_provider() : \Generator
    {
        foreach (range(1, 10) as $i) {
            yield [$i];
        }
    }

    public static function invalid_range_provider() : array
    {
        return [
            'min greater than max' => [2, 1],
        ];
    }

    public static function valid_range_provider() : array
    {
        return [
            'min equal to max' => [1, 1],
            'min less than max' => [1, 2],
            'min less than zero' => [-1, 1],
            'max and min less than zero' => [-1, -1],
        ];
    }

    public function test_can_create_random_int_from_given_range() : void
    {
        self::assertSame(1, (UniqueFactory::int(1, 1)));
        self::assertThat(
            UniqueFactory::int(1, 2),
            self::logicalOr(
                self::equalTo(1),
                self::equalTo(2)
            )
        );
    }

    /** @dataProvider integers_provider */
    public function test_can_create_random_string_with_given_length(int $expectedLength) : void
    {
        self::assertSame($expectedLength, mb_strlen(UniqueFactory::string($expectedLength)));
    }

    public function test_empty_string_on_length_below_1() : void
    {
        self::assertSame(
            '',
            UniqueFactory::string(0)
        );
        self::assertSame(
            '',
            UniqueFactory::string(-1)
        );
    }

    /** @dataProvider invalid_range_provider */
    public function test_fail_on_invalid_range(int $min, int $max) : void
    {
        self::expectException(\ValueError::class);
        UniqueFactory::int($min, $max);
    }

    /** @dataProvider valid_range_provider */
    public function test_return_random_int_on_valid_range(int $min, int $max) : void
    {
        self::assertThat(
            UniqueFactory::int($min, $max),
            self::logicalOr(
                self::greaterThanOrEqual($min),
                self::lessThanOrEqual($max)
            )
        );
    }
}
