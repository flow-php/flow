<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_enum;
use Flow\Types\Exception\{CastingException};
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Tests\Unit\Type\Fixtures\{AnotherEnum, SomeEnum};
use Flow\Types\Tests\Unit\Type\Fixtures\ColorsEnum;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class EnumTypeTest extends TestCase
{
    public static function invalid_assert_data_provider() : \Generator
    {
        yield [null, AnotherEnum::class, false];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [SomeEnum::A, SomeEnum::class];
        yield [SomeEnum::B, \UnitEnum::class]; // all enums are \UnitEnum
        yield [SomeEnum::B, \BackedEnum::class]; // SomeEnum is string backed enum
    }

    public function test_casting_integer_to_enum() : void
    {
        $this->expectException(CastingException::class);
        $this->expectExceptionMessage('Can\'t cast "int" into "enum<Flow\Types\Tests\Unit\Type\Fixtures\ColorsEnum>" type');

        type_enum(ColorsEnum::class)->cast(1);
    }

    public function test_casting_string_to_enum() : void
    {
        self::assertEquals(
            ColorsEnum::RED,
            type_enum(ColorsEnum::class)->cast('red')
        );
    }

    /**
     * @param class-string<\UnitEnum> $class
     */
    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value, string $class) : void
    {
        $this->expectException(InvalidTypeException::class);
        (type_enum($class))->assert($value);
    }

    /**
     * @param class-string<\UnitEnum> $class
     */
    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value, string $class) : void
    {
        self::assertInstanceOf($class, (type_enum($class))->assert($value));
    }
}
