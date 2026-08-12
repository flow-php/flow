<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use DateTimeImmutable;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_empty_array;
use function Flow\Types\DSL\type_from_array;

final class EmptyArrayTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid empty array' => [
            'value' => [],
            'exceptionClass' => null,
        ];

        yield 'invalid array with integer' => [
            'value' => [1],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array with key-value' => [
            'value' => ['a' => 'b'],
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string that looks like empty json list' => [
            'value' => '[]',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null' => [
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid empty string' => [
            'value' => '',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 0,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'empty array stays as is' => [
            'value' => [],
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'empty json list string' => [
            'value' => '[]',
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'empty json object string' => [
            'value' => '{}',
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'empty object' => [
            'value' => new stdClass(),
            'expected' => [],
            'exceptionClass' => null,
        ];

        yield 'non-empty array' => [
            'value' => [1],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'non-empty json list string' => [
            'value' => '[1]',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'empty string' => [
            'value' => '',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'null' => [
            'value' => null,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'integer zero' => [
            'value' => 0,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'boolean' => [
            'value' => true,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'datetime' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00 UTC'),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid json string' => [
            'value' => '{invalid json}',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'empty array' => [
            'value' => [],
            'expected' => true,
        ];

        yield 'array with string' => [
            'value' => ['one'],
            'expected' => false,
        ];

        yield 'null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'empty string' => [
            'value' => '',
            'expected' => false,
        ];

        yield 'integer zero' => [
            'value' => 0,
            'expected' => false,
        ];

        yield 'boolean false' => [
            'value' => false,
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
            type_empty_array()->assert($value);
        } else {
            static::assertSame([], type_empty_array()->assert($value));
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
            type_empty_array()->cast($value);
        } else {
            static::assertSame($expected, type_empty_array()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_empty_array()->isValid($value));
    }

    public function test_normalization(): void
    {
        static::assertSame(['type' => 'empty_array'], type_empty_array()->normalize());

        static::assertEquals(type_empty_array(), type_from_array(type_empty_array()->normalize()));
    }

    public function test_to_string(): void
    {
        static::assertSame('array{}', type_empty_array()->toString());
    }
}
