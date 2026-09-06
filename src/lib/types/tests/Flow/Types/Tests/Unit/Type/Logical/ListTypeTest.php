<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use DateTimeZone;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type\Logical\ListType;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use stdClass;

use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_uuid;

final class ListTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid list of strings' => [
            'value' => ['a', 'b'],
            'listType' => type_list(type_string()),
            'exceptionClass' => null,
        ];

        yield 'valid list with explicit keys' => [
            'value' => [0 => 'a', 1 => 'b'],
            'listType' => type_list(type_string()),
            'exceptionClass' => null,
        ];

        yield 'invalid string' => [
            'value' => 'string',
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid UUID string' => [
            'value' => '49e952c8-80ec-4910-a1d6-a19bd46b163d',
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean' => [
            'value' => false,
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float' => [
            'value' => 124.25,
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array with string keys' => [
            'value' => ['a' => 'b'],
            'listType' => type_list(type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid array with non-sequential keys' => [
            'value' => [1 => 'a', 2 => 'b'],
            'listType' => type_list(type_string()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object' => [
            'value' => new stdClass(),
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid DateTimeZone' => [
            'value' => new DateTimeZone('UTC'),
            'listType' => type_list(type_integer()),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'list of ints to list of floats' => [
            'value' => [1, 2, 3],
            'listType' => type_list(type_float()),
            'expected' => [1.0, 2.0, 3.0],
            'exceptionClass' => null,
        ];

        yield 'list of strings to list of ints' => [
            'value' => ['1'],
            'listType' => type_list(type_integer()),
            'expected' => [1],
            'exceptionClass' => null,
        ];

        yield 'JSON string payload casts element-wise' => [
            'value' => '["1","2"]',
            'listType' => type_list(type_integer()),
            'expected' => [1, 2],
            'exceptionClass' => null,
        ];

        yield 'malformed JSON string payload' => [
            'value' => '[invalid',
            'listType' => type_list(type_integer()),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'cast(null) throws' => [
            'value' => null,
            'listType' => type_list(type_string()),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'scalar is not wrapped into a single-item list' => [
            'value' => 'hello',
            'listType' => type_list(type_string()),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'null element' => [
            'value' => [null],
            'listType' => type_list(type_string()),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid list of booleans' => [
            'value' => [true, false],
            'listType' => type_list(type_boolean()),
            'expected' => true,
        ];

        yield 'valid list of strings' => [
            'value' => ['one', 'two'],
            'listType' => type_list(type_string()),
            'expected' => true,
        ];

        yield 'valid nested list of strings' => [
            'value' => [['one', 'two']],
            'listType' => type_list(type_list(type_string())),
            'expected' => true,
        ];

        yield 'valid complex nested structure' => [
            'value' => [['one' => [1, 2], 'two' => [3, 4]], ['one' => [5, 6], 'two' => [7, 8]]],
            'listType' => type_list(type_map(type_string(), type_list(type_integer()))),
            'expected' => true,
        ];

        yield 'invalid associative array' => [
            'value' => ['one' => 'two'],
            'listType' => type_list(type_string()),
            'expected' => false,
        ];

        yield 'invalid list of integers for string type' => [
            'value' => [1, 2],
            'listType' => type_list(type_string()),
            'expected' => false,
        ];

        yield 'invalid integer' => [
            'value' => 123,
            'listType' => type_list(type_string()),
            'expected' => false,
        ];

        yield 'valid empty array' => [
            'value' => [],
            'listType' => type_list(type_integer()),
            'expected' => true,
        ];
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, ListType $listType, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $listType->assert($value);
        } else {
            static::assertIsArray($listType->assert($value));
        }
    }

    /**
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, ListType $listType, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $listType->cast($value);
        } else {
            static::assertSame($expected, $listType->cast($value));
        }
    }

    public function test_a_scalar_is_not_wrapped_into_a_container(): void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('wrap the value first, e.g. type_list(type_string())');

        type_list(type_integer())->cast(5);
    }

    public function test_cast_element_failure_chains_previous(): void
    {
        try {
            type_list(type_uuid())->cast(['not-a-uuid']);
            static::fail('Expected CastingException');
        } catch (CastingException $e) {
            static::assertNotNull($e->getPrevious());
        }
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, ListType $listType, bool $expected): void
    {
        static::assertSame($expected, $listType->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_list(type_string());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_to_string(): void
    {
        static::assertSame('list<boolean>', type_list(type_boolean())->toString());
    }
}
