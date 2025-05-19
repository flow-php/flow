<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_time;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TimeTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
        yield [new \DateTimeImmutable()];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \DateInterval('PT10S')];
    }

    public static function time_castable_data_provider() : \Generator
    {
        yield 'string' => ['PT1S', new \DateInterval('PT1S')];
        yield 'datetime' => [new \DateTimeImmutable('2021-01-01 00:00:01'), new \DateInterval('PT1S')];
        yield 'date' => [new \DateTimeImmutable('2021-01-01'), new \DateInterval('PT0S')];
        yield 'time' => [new \DateInterval('PT10S'), new \DateInterval('PT10S')];
    }

    #[DataProvider('time_castable_data_provider')]
    public function test_casting_different_time_types_to_time(mixed $value, \DateInterval $expextedInterval) : void
    {
        self::assertEquals($expextedInterval, type_time()->cast($value));
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_time()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_time()->isValid(new \DateInterval('PT10S')));
        self::assertFalse(type_time()->isValid('00:00:01'));
        self::assertFalse(type_time()->isValid('PT10S'));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertInstanceOf(\DateInterval::class, type_time()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'time',
            type_time()->toString()
        );
    }
}
