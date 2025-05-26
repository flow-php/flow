<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_boolean, type_from_array};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BooleanTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid true' => [
            'value' => true,
            'exceptionClass' => null,
        ];

        yield 'valid false' => [
            'value' => false,
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'true',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 1,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string' => [
            'value' => 'string',
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'string true' => [
            'value' => 'true',
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'string 1' => [
            'value' => '1',
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'string yes' => [
            'value' => 'yes',
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'string on' => [
            'value' => 'on',
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'string false' => [
            'value' => 'false',
            'expected' => false,
            'exceptionClass' => null,
        ];

        yield 'string 0' => [
            'value' => '0',
            'expected' => false,
            'exceptionClass' => null,
        ];

        yield 'string no' => [
            'value' => 'no',
            'expected' => false,
            'exceptionClass' => null,
        ];

        yield 'string off' => [
            'value' => 'off',
            'expected' => false,
            'exceptionClass' => null,
        ];

        yield 'int' => [
            'value' => 1,
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'float' => [
            'value' => 1.1,
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'bool' => [
            'value' => true,
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'array' => [
            'value' => [1, 2, 3],
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'DateTimeInterface' => [
            'value' => new \DateTimeImmutable('2021-01-01 00:00:00'),
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'DateInterval' => [
            'value' => new \DateInterval('P1D'),
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'DOMDocument' => [
            'value' => new \DOMDocument(),
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'DOMElement - true' => [
            'value' => new \DOMElement('element', 'true'),
            'expected' => true,
            'exceptionClass' => null,
        ];

        yield 'DOMElement - false' => [
            'value' => new \DOMElement('element', 'false'),
            'expected' => false,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid true' => [
            'value' => true,
            'expected' => true,
        ];

        yield 'valid false' => [
            'value' => false,
            'expected' => true,
        ];

        yield 'invalid string' => [
            'value' => 'one',
            'expected' => false,
        ];

        yield 'invalid array' => [
            'value' => [1, 2],
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_boolean()->assert($value);
        } else {
            self::assertIsBool(type_boolean()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_boolean()->cast($value);
        } else {
            self::assertSame($expected, type_boolean()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_boolean()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_boolean();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'boolean',
            type_boolean()->toString()
        );
    }
}
