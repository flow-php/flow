<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\type_float;
use Flow\ETL\Exception\InvalidTypeException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class FloatTypeTest extends TestCase
{
    public static function float_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', 0.0];
        yield 'int' => [1, 1.0];
        yield 'float' => [1.1, 1.1];
        yield 'bool' => [true, 1.0];
        yield 'array' => [[1, 2, 3], 1.0];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), 1609459200000000.0];
        yield 'DateInterval' => [new \DateInterval('P1D'), 86400000000.0];
        yield 'DOMElement' => [new \DOMElement('element', '1.1'), 1.1];
    }

    public static function invalid_assert_data_provider() : \Generator
    {
        yield ['string'];
        yield [false];
        yield [123];
        yield [[1, 2]];
        yield [new \stdClass()];
        yield [new \DateTimeImmutable()];
        yield [new \DateTime()];
        yield [new \DateTimeZone('UTC')];
    }

    public static function successful_assert_data_provider() : \Generator
    {
        yield [1234.52];
        yield [-1234.52];
        yield [1.22e-15];
        yield [-1.22e-15];
        yield [.25];
        yield [-.25];
    }

    #[DataProvider('float_castable_data_provider')]
    public function test_casting_different_data_types_to_float(mixed $value, float $expected) : void
    {
        self::assertSame($expected, type_float()->cast($value));
    }

    #[DataProvider('invalid_assert_data_provider')]
    public function test_invalid_assert(mixed $value) : void
    {
        $this->expectException(InvalidTypeException::class);
        type_float()->assert($value);
    }

    public function test_normalize() : void
    {
        self::assertSame(
            [
                'type' => 'float',
            ],
            type_float()->normalize()
        );
    }

    #[DataProvider('successful_assert_data_provider')]
    public function test_successful_assert(mixed $value) : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsFloat(type_float()->assert($value));
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'float',
            type_float()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_float()->isValid(1.0)
        );
        self::assertFalse(
            type_float()->isValid('one')
        );
        self::assertFalse(
            type_float()->isValid([1, 2])
        );
        self::assertFalse(
            type_float()->isValid(123)
        );
    }
}
