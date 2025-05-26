<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_from_array, type_object};
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ObjectTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'exceptionClass' => null,
        ];

        yield 'valid DateTime' => [
            'value' => new \DateTime(),
            'exceptionClass' => null,
        ];

        yield 'valid stdClass' => [
            'value' => new \stdClass(),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
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
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string to object' => [
            'value' => 'string',
            'expected' => (object) 'string',
            'exceptionClass' => null,
        ];

        yield 'array to object' => [
            'value' => ['foo' => 'bar'],
            'expected' => json_decode('{"foo":"bar"}'),
            'exceptionClass' => null,
        ];

        yield 'object stays as is' => [
            'value' => $obj = new \stdClass(),
            'expected' => $obj,
            'exceptionClass' => null,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid stdClass' => [
            'value' => new \stdClass(),
            'expected' => true,
        ];

        yield 'valid DateTimeImmutable' => [
            'value' => new \DateTimeImmutable(),
            'expected' => true,
        ];

        yield 'valid DateTime' => [
            'value' => new \DateTime(),
            'expected' => true,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'expected' => false,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'expected' => false,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'expected' => false,
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_object()->assert($value);
        } else {
            self::assertIsObject(type_object()->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_object()->cast($value);
        } else {
            self::assertEquals($expected, type_object()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_object()->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_object();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'object',
            type_object()->toString()
        );
    }
}
