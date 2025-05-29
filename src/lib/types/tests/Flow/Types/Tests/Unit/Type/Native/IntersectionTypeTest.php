<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{
    type_from_array,
    type_instance_of,
    type_integer,
    type_intersection,
    type_mixed,
    type_positive_integer,
    types};
use Flow\Types\Exception\{CastingException, InvalidTypeException};
use Flow\Types\Tests\Unit\Type\Fixtures\Intersection\{Date, DateOrTime, Time};
use Flow\Types\Type\Native\IntersectionType;
use Flow\Types\Type\Types;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class IntersectionTypeTest extends TestCase
{
    public static function assert_data_provider() : \Generator
    {
        yield 'valid integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 1,
            'exceptionClass' => null,
        ];

        yield 'invalid negative integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => -1,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 'abc',
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid null for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => null,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid boolean for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => false,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid float for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 1.0,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid object for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'valid DateOrTime object for Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new DateOrTime(),
            'exceptionClass' => null,
        ];

        yield 'invalid stdClass for Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new \stdClass(),
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider() : \Generator
    {
        yield 'string to integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => '1',
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'float to integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 1.0,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'boolean to integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => true,
            'expected' => 1,
            'exceptionClass' => null,
        ];

        yield 'negative string to integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => '-1',
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'DateOrTime object stays as is for Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new DateOrTime(),
            'expected' => new DateOrTime(),
            'exceptionClass' => null,
        ];

        yield 'stdClass cannot be cast to Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new \stdClass(),
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield 'valid positive integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 1,
            'expected' => true,
        ];

        yield 'invalid negative integer for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => -1,
            'expected' => false,
        ];

        yield 'invalid string for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 'abc',
            'expected' => false,
        ];

        yield 'invalid null for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => null,
            'expected' => false,
        ];

        yield 'invalid boolean for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => false,
            'expected' => false,
        ];

        yield 'invalid float for integer&positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'value' => 1.0,
            'expected' => false,
        ];

        yield 'valid DateOrTime object for Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new DateOrTime(),
            'expected' => true,
        ];

        yield 'invalid stdClass for Date&Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'value' => new \stdClass(),
            'expected' => false,
        ];
    }

    public static function to_string_data_provider() : \Generator
    {
        yield 'integer and positive_integer' => [
            'type' => type_intersection(type_integer(), type_positive_integer()),
            'expected' => 'intersection<integer&positive_integer>',
        ];

        yield 'multiple integers and positive_integer' => [
            'type' => type_intersection(type_integer(), type_integer(), type_integer(), type_intersection(type_integer(), type_positive_integer())),
            'expected' => 'intersection<integer&positive_integer>',
        ];

        yield 'Date and Time' => [
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
            'expected' => 'intersection<object<' . Date::class . '>&object<' . Time::class . '>>',
        ];
    }

    public static function types_data_provider() : \Generator
    {
        yield 'integer and positive_integer' => [
            'expected' => types(type_integer(), type_positive_integer()),
            'type' => type_intersection(type_integer(), type_positive_integer()),
        ];

        yield 'multiple integers and positive_integer' => [
            'expected' => types(type_integer(), type_integer(), type_positive_integer()),
            'type' => type_intersection(type_integer(), type_integer(), type_positive_integer()),
        ];

        yield 'Date and Time' => [
            'expected' => types(type_instance_of(Date::class), type_instance_of(Time::class)),
            'type' => type_intersection(type_instance_of(Date::class), type_instance_of(Time::class)),
        ];
    }

    #[DataProvider('assert_data_provider')]
    public function test_assert(IntersectionType $type, mixed $value, ?string $exceptionClass = null) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->assert($value);
        } else {
            self::assertSame($value, $type->assert($value));
        }
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(IntersectionType $type, mixed $value, mixed $expected, ?string $exceptionClass) : void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            $type->cast($value);
        } else {
            self::assertEquals($expected, $type->cast($value));
        }
    }

    public function test_intersection_with_mixed_type_as_left() : void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('IntersectionType cannot be mixed with MixedType, mixed is a standalone type');

        type_intersection(type_mixed(), type_integer());
    }

    public function test_intersection_with_mixed_type_as_right() : void
    {
        $this->expectException(InvalidTypeException::class);
        $this->expectExceptionMessage('IntersectionType cannot be mixed with MixedType, mixed is a standalone type');

        type_intersection(type_integer(), type_mixed());
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(IntersectionType $type, mixed $value, bool $expected) : void
    {
        self::assertSame($expected, $type->isValid($value));
    }

    public function test_normalization() : void
    {
        $type = type_intersection(type_integer(), type_positive_integer());
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        self::assertEquals($type, $recreated);
    }

    #[DataProvider('to_string_data_provider')]
    public function test_to_string(IntersectionType $type, string $expected) : void
    {
        self::assertSame($expected, $type->toString());
    }

    #[DataProvider('types_data_provider')]
    public function test_types(Types $expected, IntersectionType $type) : void
    {
        self::assertEquals($expected, $type->types());
    }
}
