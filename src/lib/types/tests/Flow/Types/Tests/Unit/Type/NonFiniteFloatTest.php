<?php

declare(strict_types=1);

namespace Flow\Types\Tests\Unit\Type;

use Flow\Types\Type\NonFiniteFloat;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class NonFiniteFloatTest extends TestCase
{
    #[TestWith([NAN, 'NAN'])]
    #[TestWith([INF, 'INF'])]
    #[TestWith([-INF, '-INF'])]
    public function test_text_round_trips_through_from_text(float $value, string $text): void
    {
        static::assertSame($text, NonFiniteFloat::text($value));
        static::assertSame($text, NonFiniteFloat::text(NonFiniteFloat::fromText($text) ?? 0.0));
    }

    #[TestWith([0.0])]
    #[TestWith([-1.5])]
    #[TestWith([PHP_FLOAT_MAX])]
    public function test_a_finite_float_has_no_text(float $value): void
    {
        static::assertNull(NonFiniteFloat::text($value));
    }

    #[TestWith(['nan'])]
    #[TestWith(['Inf'])]
    #[TestWith(['+INF'])]
    #[TestWith(['1.5'])]
    #[TestWith([''])]
    public function test_any_other_text_is_no_non_finite_float(string $text): void
    {
        static::assertNull(NonFiniteFloat::fromText($text));
    }

    #[TestWith([NAN, true])]
    #[TestWith([INF, true])]
    #[TestWith([-INF, true])]
    #[TestWith([1.5, false])]
    #[TestWith([1, false])]
    #[TestWith(['NAN', false])]
    #[TestWith([null, false])]
    public function test_is_only_a_float_that_is_not_finite(mixed $value, bool $is): void
    {
        static::assertSame($is, NonFiniteFloat::is($value));
    }
}
