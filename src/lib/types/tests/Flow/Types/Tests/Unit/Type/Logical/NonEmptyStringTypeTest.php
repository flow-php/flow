<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use DOMElement;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_non_empty_string;

final class NonEmptyStringTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid non-empty string' => [
            'value' => 'string',
            'exceptionClass' => null,
        ];

        yield 'invalid empty string' => [
            'value' => '',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 0,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'string stays as is' => [
            'value' => 'string',
            'expected' => 'string',
            'exceptionClass' => null,
        ];

        yield 'empty string throws exception' => [
            'value' => '',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'integer 0 to string' => [
            'value' => 0,
            'expected' => '0',
            'exceptionClass' => null,
        ];

        yield 'integer 1 to string' => [
            'value' => 1,
            'expected' => '1',
            'exceptionClass' => null,
        ];

        yield 'DateTimeImmutable to string' => [
            'value' => new DateTimeImmutable('2024-12-01'),
            'expected' => '2024-12-01T00:00:00+00:00',
            'exceptionClass' => null,
        ];

        yield 'DateTime to string' => [
            'value' => new DateTime('2024-12-01'),
            'expected' => '2024-12-01T00:00:00+00:00',
            'exceptionClass' => null,
        ];

        yield 'DateTimeZone to string' => [
            'value' => new DateTimeZone('UTC'),
            'expected' => 'UTC',
            'exceptionClass' => null,
        ];

        yield 'DOMElement to string' => [
            'value' => new DOMElement('element', '2024-12-01'),
            'expected' => '<element>2024-12-01</element>',
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid non-empty string' => [
            'value' => 'string',
            'expected' => true,
        ];

        yield 'invalid empty string' => [
            'value' => '',
            'expected' => false,
        ];

        yield 'invalid integer 0' => [
            'value' => 0,
            'expected' => false,
        ];

        yield 'invalid integer 1' => [
            'value' => 1,
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
            type_non_empty_string()->assert($value);
        } else {
            $result = type_non_empty_string()->assert($value);
            static::assertIsString($result);
            static::assertSame($value, $result);
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
            type_non_empty_string()->cast($value);
        } else {
            static::assertSame($expected, type_non_empty_string()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        static::assertSame($expected, type_non_empty_string()->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_non_empty_string();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('non_empty_string', type_non_empty_string()->toString());
    }
}
