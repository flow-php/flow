<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_time};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class TimeTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid DateInterval' => [
            'value' => new \DateInterval('PT10S'),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new \DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string to time' => [
            'value' => 'PT1S',
            'expected' => new \DateInterval('PT1S'),
            'exceptionClass' => null,
        ];

        yield 'datetime to time' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:01'),
            'expected' => new \DateInterval('PT1S'),
            'exceptionClass' => null,
        ];

        yield 'date to time' => [
            'value' => new \DateTimeImmutable('2021-01-01'),
            'expected' => new \DateInterval('PT0S'),
            'exceptionClass' => null,
        ];

        yield 'time stays as is' => [
            'value' => new \DateInterval('PT10S'),
            'expected' => new \DateInterval('PT10S'),
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid DateInterval' => [
            'value' => new \DateInterval('PT10S'),
            'expected' => true,
        ];

        yield 'invalid time string' => [
            'value' => '00:00:01',
            'expected' => false,
        ];

        yield 'invalid interval string' => [
            'value' => 'PT10S',
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_time()->assert($value);
        } else {
            self::assertInstanceOf(\DateInterval::class, type_time()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_time()->cast($value);
        } else {
            self::assertEquals($expected, type_time()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_time()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_time();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'time',
            type_time()->toString()
        );
    }
}
