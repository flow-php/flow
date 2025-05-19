<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_integer;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\{DataProvider};
use PHPUnit\Framework\TestCase;

final class IntegerTypeTest extends TestCase
{
    public static function integer_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 0];
        yield 'int' => [1, 1];
        yield 'float' => [1.1, 1];
        yield 'bool' => [true, 1];
        yield 'array' => [[1, 2, 3], 1];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), 1609459200000000];
        yield 'DateInterval' => [new \DateInterval('P1D'), 86400000000];
        yield 'DOMElement' => [new \DOMElement('element', '1'), 1];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield [false];
        yield [124.25];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [1234];
        yield [1234];
        yield [PHP_INT_MAX];
    }

    #[DataProvider('integer_castable_data_provider')]
    public function test_casting_different_data_types_to_integer(mixed $value, int $expected) : void
    {
        self::assertSame($expected, type_integer()->cast($value));
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_integer()->assert($value);
    }

    public function test_normalize() : void
    {
        self::assertSame(
            ['type' => 'integer'],
            type_integer()->normalize()
        );

    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsInt(type_integer()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'integer',
            type_integer()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_integer()->isValid(1)
        );
        self::assertFalse(
            type_integer()->isValid('one')
        );
        self::assertFalse(
            type_integer()->isValid([1, 2])
        );
        self::assertFalse(
            type_integer()->isValid(123.0)
        );
    }
}
