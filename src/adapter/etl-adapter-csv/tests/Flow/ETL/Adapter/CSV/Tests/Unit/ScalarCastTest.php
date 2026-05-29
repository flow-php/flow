<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\Tests\Unit;

use Flow\ETL\Adapter\CSV\RowsNormalizer\ScalarCast;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use stdClass;

final class ScalarCastTest extends FlowTestCase
{
    public function test_string_value(): void
    {
        static::assertSame('hello', ScalarCast::fromMixed('hello'));
    }

    public function test_empty_string_value(): void
    {
        static::assertSame('', ScalarCast::fromMixed(''));
    }

    public function test_integer_value(): void
    {
        static::assertSame(42, ScalarCast::fromMixed(42));
    }

    public function test_float_value(): void
    {
        static::assertSame(3.14, ScalarCast::fromMixed(3.14));
    }

    public function test_boolean_true(): void
    {
        static::assertTrue(ScalarCast::fromMixed(true));
    }

    public function test_boolean_false(): void
    {
        static::assertFalse(ScalarCast::fromMixed(false));
    }

    public function test_null_value(): void
    {
        static::assertNull(ScalarCast::fromMixed(null));
    }

    public function test_object_with_to_string(): void
    {
        $object = new class {
            public function __toString(): string
            {
                return 'stringable';
            }
        };

        static::assertSame('stringable', ScalarCast::fromMixed($object));
    }

    public function test_array_returns_empty_string(): void
    {
        static::assertSame('', ScalarCast::fromMixed([1, 2, 3]));
    }

    public function test_object_without_to_string_returns_empty_string(): void
    {
        static::assertSame('', ScalarCast::fromMixed(new stdClass()));
    }

    #[TestWith([0])]
    #[TestWith([-1])]
    #[TestWith([PHP_INT_MAX])]
    public function test_edge_case_integers(int $value): void
    {
        static::assertSame($value, ScalarCast::fromMixed($value));
    }

    #[TestWith([0.0])]
    #[TestWith([-0.0])]
    #[TestWith([PHP_FLOAT_MAX])]
    public function test_edge_case_floats(float $value): void
    {
        static::assertSame($value, ScalarCast::fromMixed($value));
    }
}
