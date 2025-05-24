<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type\Native;

use function Flow\Types\DSL\{type_from_array, type_mixed};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class MixedTypeTest extends TestCase
{
    public static function mixed_data_provider() : \Generator
    {
        yield [null, null];
        yield [true, true];
        yield [false, false];
        yield [0, 0];
        yield [1, 1];
        yield [0.0, 0.0];
        yield [1.0, 1.0];
        yield ['string', 'string'];
        yield [[], []];
        yield [[1], [1]];
        yield [[1 => 2], [1 => 2]];
        yield [(object) [], (object) []];
        yield [(object) ['a' => 'b'], (object) ['a' => 'b']];
    }

    #[DataProvider('mixed_data_provider')]
    public function test_assert_is_valid_and_cast(mixed $value, mixed $expected) : void
    {
        self::assertEquals($expected, type_mixed()->assert($value));
        self::assertEquals($expected, type_mixed()->cast($value));
        self::assertTrue(type_mixed()->isValid($value));
    }

    public function test_normalize() : void
    {
        self::assertEquals(['type' => 'mixed'], type_mixed()->normalize());

        self::assertEquals(
            type_mixed(),
            type_from_array(type_mixed()->normalize())
        );
    }

    public function test_to_string() : void
    {
        self::assertEquals('mixed', type_mixed()->toString());
    }
}
