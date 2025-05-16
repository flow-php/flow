<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_boolean, type_from_array};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class BooleanTypeTest extends TestCase
{
    public static function boolean_castable_data_provider() : \Generator
    {
        yield 'string' => ['string', true];
        yield 'string true' => ['true', true];
        yield 'string 1' => ['1', true];
        yield 'string yes' => ['yes', true];
        yield 'string on' => ['on', true];
        yield 'string false' => ['false', false];
        yield 'string 0' => ['0', false];
        yield 'string no' => ['no', false];
        yield 'string off' => ['off', false];
        yield 'int' => [1, true];
        yield 'float' => [1.1, true];
        yield 'bool' => [true, true];
        yield 'array' => [[1, 2, 3], true];
        yield 'DateTimeInterface' => [new \DateTimeImmutable('2021-01-01 00:00:00'), true];
        yield 'DateInterval' => [new \DateInterval('P1D'), true];
        yield 'DOMDocument' => [new \DOMDocument(), true];
        yield 'DOMElement - true' => [new \DOMElement('element', 'true'), true];
        yield 'DOMElement - false' => [new \DOMElement('element', 'false'), false];
    }

    #[DataProvider('boolean_castable_data_provider')]
    public function test_casting_different_data_types_to_integer(mixed $value, bool $expected) : void
    {
        self::assertSame($expected, type_boolean()->cast($value));
    }

    public function test_invalid_assertion() : void
    {
        $this->expectExceptionMessage('Expected type "boolean", got "string".');
        type_boolean()->assert('true');
    }

    public function test_normalization() : void
    {
        self::assertEquals(
            [
                'type' => 'boolean',
            ],
            type_boolean()->normalize()
        );

        self::assertEquals(
            type_boolean(),
            type_from_array(type_boolean()->normalize())
        );
    }

    public function test_normalize() : void
    {
        self::assertSame(
            ['type' => 'boolean'],
            type_boolean()->normalize()
        );
        self::assertSame(
            ['type' => 'boolean'],
            type_boolean()->normalize()
        );
    }

    public function test_to_string() : void
    {
        self::assertSame(
            'boolean',
            type_boolean()->toString()
        );
    }

    public function test_valid() : void
    {
        self::assertTrue(
            type_boolean()->isValid(true)
        );

        self::assertFalse(
            type_boolean()->isValid('one')
        );
        self::assertFalse(
            type_boolean()->isValid([1, 2])
        );
        self::assertFalse(
            type_boolean()->isValid(123)
        );
    }

    public function test_valid_assertion() : void
    {
        /** @phpstan-ignore-next-line */
        self::assertIsBool(type_boolean()->assert(true));
        /** @phpstan-ignore-next-line */
        self::assertIsBool(type_boolean()->assert(false));
    }
}
