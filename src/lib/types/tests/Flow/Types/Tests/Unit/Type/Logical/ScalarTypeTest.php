<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Logical;

use function Flow\Types\DSL\type_scalar;
use Flow\ETL\Exception\InvalidTypeException;
use Flow\Types\Type\TypeFactory;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ScalarTypeTest extends TestCase
{
    public static function cast_data_provider() : \Generator
    {
        yield ['string', 'string'];
        yield [1, 1];
        yield [1.0, 1.0];
        yield [true, true];
        yield [false, false];
        yield [null, ''];
        yield [[], '[]'];
        yield [new \stdClass(), 1];
    }

    public static function is_valid_data_provider() : \Generator
    {
        yield ['string', true];
        yield [1, true];
        yield [1.0, true];
        yield [true, true];
        yield [false, true];
        yield [null, false];
        yield [[], false];
        yield [new \stdClass(), false];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield [1];
        yield [1.0];
        yield [true];
        yield [false];
    }

    public static function unsuccessful_assert_data_provider() : \Generator
    {
        yield [null];
        yield [[]];
        yield [new \stdClass()];
    }

    #[DataProvider('cast_data_provider')]
    public function test_cast(mixed $input, mixed $expected) : void
    {
        self::assertSame($expected, type_scalar()->cast($input));
    }

    #[DataProvider('is_valid_data_provider')]
    public function test_is_valid(mixed $value, bool $result) : void
    {
        self::assertSame($result, type_scalar()->isValid($value));
    }

    public function test_normalize() : void
    {
        $scalarType = type_scalar();

        self::assertSame(
            [
                'type' => 'scalar',
            ],
            $scalarType->normalize()
        );

        self::assertEquals(
            type_scalar(),
            TypeFactory::fromArray(['type' => 'scalar'])
        );
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        self::assertSame($value, type_scalar()->assert($value));
    }

    #[DataProvider('unsuccessful_assert_data_provider')]
    public function test_unsuccessful_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_scalar()->assert($value);
    }
}
