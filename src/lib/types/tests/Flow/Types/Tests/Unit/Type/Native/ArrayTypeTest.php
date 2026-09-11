<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use DateTimeImmutable;
use DOMDocument;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_from_array;

final class ArrayTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid empty array' => [
            'value' => [],
            'exceptionClass' => null,
        ];

        yield 'valid array with integer' => [
            'value' => [1],
            'exceptionClass' => null,
        ];

        yield 'valid array with key-value' => [
            'value' => ['a' => 'b'],
            'exceptionClass' => null,
        ];

        yield 'invalid string that looks like json' => [
            'value' => '[]',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null' => [
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string' => [
            'value' => 'one',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => true,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'array stays as is' => [
            'value' => ['test'],
            'expected' => ['test'],
            'exceptionClass' => null,
        ];

        yield 'boolean to array' => [
            'value' => true,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'datetime to array' => [
            'value' => new DateTimeImmutable('2021-01-01 00:00:00 UTC'),
            'expected' => ['date' => '2021-01-01 00:00:00.000000', 'timezone_type' => 3, 'timezone' => 'UTC'],
            'exceptionClass' => null,
        ];

        yield 'float to array' => [
            'value' => 1.1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'integer to array' => [
            'value' => 1,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'json string to array' => [
            'value' => '{"items":{"item":1}}',
            'expected' => ['items' => ['item' => 1]],
            'exceptionClass' => null,
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
            'expected' => true,
        ];

        yield 'array with integer' => [
            'value' => [1],
            'expected' => true,
        ];

        yield 'null' => [
            'value' => null,
            'expected' => false,
        ];

        yield 'string' => [
            'value' => 'one',
            'expected' => false,
        ];

        yield 'boolean' => [
            'value' => true,
            'expected' => false,
        ];

        yield 'integer' => [
            'value' => 123,
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
            type_array()->assert($value);
        } else {
            static::assertIsArray(type_array()->assert($value));
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
            type_array()->cast($value);
        } else {
            static::assertEquals($expected, type_array()->cast($value));
        }
    }

    public function test_a_scalar_is_not_wrapped_into_a_container(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('wrap the value first, e.g. type_list(type_string())');

        type_array()->cast('abc');
    }

    public function test_casting_xml_document_to_array(): void
    {
        $xml = new DOMDocument();
        $xml->loadXML('<root><foo baz="buz">bar</foo></root>');

        static::assertSame(
            ['root' => ['foo' => ['@attributes' => ['baz' => 'buz'], '@value' => 'bar']]],
            type_array()->cast($xml),
        );
    }

    public function test_null_does_not_become_an_empty_array(): void
    {
        $this->expectException(CastingException::class);

        type_array()->cast(null);
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_array()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_array();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('array<mixed>', type_array()->toString());
    }
}
