<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\{type_from_array, type_json};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class JsonTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid JSON array' => [
            'value' => '[1, 2]',
            'exceptionClass' => null,
        ];

        yield 'valid JSON object' => [
            'value' => '{"foo": "bar"}',
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
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'array to JSON' => [
            'value' => ['items' => ['item' => 1]],
            'expected' => '{"items":{"item":1}}',
            'exceptionClass' => null,
        ];

        yield 'DateTimeImmutable to JSON' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:00 UTC'),
            'expected' => '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            'exceptionClass' => null,
        ];

        yield 'JSON string to JSON' => [
            'value' => '{"items":{"item":1}}',
            'expected' => '{"items":{"item":1}}',
            'exceptionClass' => null,
        ];

        yield 'integer to JSON' => [
            'value' => 1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'non-JSON string to JSON' => [
            'value' => 'string',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid JSON object' => [
            'value' => '{"foo": "bar"}',
            'expected' => true,
        ];

        yield 'invalid incomplete JSON' => [
            'value' => '{"foo": "bar"',
            'expected' => false,
        ];

        yield 'invalid numeric string' => [
            'value' => '2',
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_json()->assert($value);
        } else {
            self::assertIsString(type_json()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_json()->cast($value);
        } else {
            self::assertSame($expected, type_json()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_json()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_json();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'json',
            type_json()->toString()
        );
    }
}
