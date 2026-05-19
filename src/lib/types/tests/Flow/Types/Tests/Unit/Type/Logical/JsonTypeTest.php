<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Value\Json;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_json;

final class JsonTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid Json instance' => [
            'value' => new Json('{"foo": "bar"}'),
            'exceptionClass' => null,
        ];

        yield 'invalid string (even valid JSON string)' => [
            'value' => '{"foo": "bar"}',
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
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'array to Json' => [
            'value' => ['items' => ['item' => 1]],
            'expected' => '{"items":{"item":1}}',
            'exceptionClass' => null,
        ];

        yield 'DateTimeImmutable to Json' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00 UTC'),
            'expected' => '{"date":"2021-01-01 00:00:00.000000","timezone_type":3,"timezone":"UTC"}',
            'exceptionClass' => null,
        ];

        yield 'JSON string to Json' => [
            'value' => '{"items":{"item":1}}',
            'expected' => '{"items":{"item":1}}',
            'exceptionClass' => null,
        ];

        yield 'Json instance to Json' => [
            'value' => new Json('{"foo":"bar"}'),
            'expected' => '{"foo":"bar"}',
            'exceptionClass' => null,
        ];

        yield 'integer to Json' => [
            'value' => 1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'non-JSON string to Json' => [
            'value' => 'string',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid Json instance' => [
            'value' => new Json('{"foo": "bar"}'),
            'expected' => true,
        ];

        yield 'invalid JSON string (strings are not valid anymore)' => [
            'value' => '{"foo": "bar"}',
            'expected' => false,
        ];

        yield 'invalid incomplete JSON' => [
            'value' => '{"foo": "bar"',
            'expected' => false,
        ];

        yield 'invalid numeric string' => [
            'value' => '2',
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'expected' => false,
        ];
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_json()->assert($value);
        } else {
            static::assertInstanceOf(Json::class, type_json()->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_json()->cast($value);
        } else {
            $result = type_json()->cast($value);
            static::assertInstanceOf(Json::class, $result);
            static::assertSame($expected, $result->toString());
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_json()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_json();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('json', type_json()->toString());
    }
}
