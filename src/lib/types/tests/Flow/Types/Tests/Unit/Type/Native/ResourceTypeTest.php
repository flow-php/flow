<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use DateTime;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function fclose;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_resource;
use function fopen;

final class ResourceTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid resource' => [
            'value' => fopen('php://temp/max', 'r+b'),
            'exceptionClass' => null,
        ];

        yield 'invalid null' => [
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer' => [
            'value' => 123,
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

        yield 'invalid DateTimeImmutable' => [
            'value' => new DateTimeImmutable(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTime' => [
            'value' => new DateTime(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        $resource = fopen('php://temp/max', 'r+b');

        yield 'resource stays as is' => [
            'value' => $resource,
            'expected' => $resource,
            'exceptionClass' => null,
        ];

        yield 'string to resource' => [
            'value' => 'not a resource',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'array to resource' => [
            'value' => [],
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid resource' => [
            'value' => fopen('php://temp/max', 'r+b'),
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

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_resource()->assert($value);
        } else {
            static::assertIsResource(type_resource()->assert($value));
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
            type_resource()->cast($value);
        } else {
            static::assertSame($expected, type_resource()->cast($value));
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $expected): void
    {
        try {
            static::assertSame($expected, type_resource()->isValid($value));
        } finally {
            if (is_resource($value)) {
                fclose($value);
            }
        }
    }

    public function test_normalization(): void
    {
        $type = type_resource();
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('resource', type_resource()->toString());
    }
}
