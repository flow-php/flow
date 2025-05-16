<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_datetime;
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DateTimeTypeTest extends TestCase
{
    public static function datetime_castable_data_provider() : \Generator
    {
        yield 'string' => ['2021-01-01 00:00:00', new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'int' => [1609459200, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'float' => [1609459200.0, new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'bool' => [true, new \DateTimeImmutable('1970-01-01 00:00:01')];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
        yield 'DateInterval' => [new \DateInterval('P1D'), new \DateTimeImmutable('1970-01-02 00:00:00')];
        yield 'DOMElement' => [new \DOMElement('element', '2021-01-01 00:00:00'), new \DateTimeImmutable('2021-01-01 00:00:00')];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield ['49e952c8-80ec-4910-a1d6-a19bd46b163d'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
    }

    #[DataProvider('datetime_castable_data_provider')]
    public function test_casting_different_data_types_to_datetime(mixed $value, \DateTimeImmutable $expected) : void
    {
        self::assertEquals($expected, type_datetime()->cast($value));
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_datetime()->assert($value);
    }

    public function test_is_valid() : void
    {
        self::assertTrue(type_datetime()->isValid(new \DateTimeImmutable()));
        self::assertTrue(type_datetime()->isValid(new \DateTime()));
        self::assertFalse(type_datetime()->isValid('2020-01-01'));
        self::assertFalse(type_datetime()->isValid('2020-01-01 00:00:00'));
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertInstanceOf(\DateTimeInterface::class, type_datetime()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'datetime',
            type_datetime()->toString()
        );
    }
}
