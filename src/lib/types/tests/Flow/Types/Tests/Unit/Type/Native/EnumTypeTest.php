<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use BackedEnum;
use Flow\Types\Exception\CastingException;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Tests\Unit\Type\Fixtures\AnotherEnum;
use Flow\Types\Tests\Unit\Type\Fixtures\ColorsEnum;
use Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use UnitEnum;

use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_from_array;

final class EnumTypeTest extends TestCase
{
    public static function assert_data_provider(): Generator
    {
        yield 'valid enum value' => [
            'value' => SomeEnum::A,
            'class' => SomeEnum::class,
            'exceptionClass' => null,
        ];

        yield 'valid enum for UnitEnum' => [
            'value' => SomeEnum::B,
            'class' => UnitEnum::class,
            'exceptionClass' => null,
        ];

        yield 'valid enum for BackedEnum' => [
            'value' => SomeEnum::B,
            'class' => BackedEnum::class,
            'exceptionClass' => null,
        ];

        yield 'invalid null value' => [
            'value' => null,
            'class' => AnotherEnum::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid string value' => [
            'value' => 'not_an_enum',
            'class' => SomeEnum::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid integer value' => [
            'value' => 123,
            'class' => ColorsEnum::class,
            'exceptionClass' => InvalidTypeException::class,
        ];

        yield 'invalid class-string of the enum itself' => [
            'value' => SomeEnum::class,
            'class' => SomeEnum::class,
            'exceptionClass' => InvalidTypeException::class,
        ];
    }

    public static function cast_data_provider(): Generator
    {
        yield 'valid string to enum' => [
            'value' => 'red',
            'class' => ColorsEnum::class,
            'expected' => ColorsEnum::RED,
            'exceptionClass' => null,
        ];

        yield 'valid enum stays as is' => [
            'value' => SomeEnum::A,
            'class' => SomeEnum::class,
            'expected' => SomeEnum::A,
            'exceptionClass' => null,
        ];

        yield 'invalid integer to enum' => [
            'value' => 1,
            'class' => ColorsEnum::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid string to enum' => [
            'value' => 'not_a_color',
            'class' => ColorsEnum::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];

        yield 'invalid class-string of the enum itself' => [
            'value' => SomeEnum::class,
            'class' => SomeEnum::class,
            'expected' => null,
            'exceptionClass' => CastingException::class,
        ];
    }

    public static function is_valid_data_provider(): Generator
    {
        yield 'valid enum value' => [
            'value' => SomeEnum::A,
            'class' => SomeEnum::class,
            'expected' => true,
        ];

        yield 'valid enum for UnitEnum' => [
            'value' => SomeEnum::B,
            'class' => UnitEnum::class,
            'expected' => true,
        ];

        yield 'invalid null value' => [
            'value' => null,
            'class' => AnotherEnum::class,
            'expected' => false,
        ];

        yield 'invalid string value' => [
            'value' => 'not_an_enum',
            'class' => SomeEnum::class,
            'expected' => false,
        ];

        yield 'invalid integer value' => [
            'value' => 123,
            'class' => ColorsEnum::class,
            'expected' => false,
        ];

        yield 'invalid class-string of the enum itself' => [
            'value' => SomeEnum::class,
            'class' => SomeEnum::class,
            'expected' => false,
        ];
    }

    /**
     * @param class-string<\UnitEnum> $class
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('assert_data_provider')]
    public function test_assert(mixed $value, string $class, ?string $exceptionClass = null): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_enum($class)->assert($value);
        } else {
            static::assertInstanceOf($class, type_enum($class)->assert($value));
        }
    }

    /**
     * @param class-string<\UnitEnum> $class
     * @param null|class-string<\Throwable> $exceptionClass
     */
    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $value, string $class, mixed $expected, ?string $exceptionClass): void
    {
        if ($exceptionClass !== null) {
            $this->expectException($exceptionClass);
            type_enum($class)->cast($value);
        } else {
            static::assertSame($expected, type_enum($class)->cast($value));
        }
    }

    /**
     * @param class-string<\UnitEnum> $class
     */
    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, string $class, bool $expected): void
    {
        static::assertSame($expected, type_enum($class)->isValid($value));
    }

    public function test_normalization(): void
    {
        $type = type_enum(SomeEnum::class);
        $normalized = $type->normalize();
        $recreated = type_from_array($normalized);

        static::assertEquals($type, $recreated);
    }

    public function test_normalization_of_backed_enum_interface(): void
    {
        $type = type_enum(BackedEnum::class);

        static::assertEquals($type, type_from_array($type->normalize()));
    }

    public function test_normalization_of_unit_enum_interface(): void
    {
        $type = type_enum(UnitEnum::class);

        static::assertEquals($type, type_from_array($type->normalize()));
    }

    public function test_to_string(): void
    {
        static::assertSame(
            'enum<Flow\Types\Tests\Unit\Type\Fixtures\SomeEnum>',
            type_enum(SomeEnum::class)->toString(),
        );
    }
}
