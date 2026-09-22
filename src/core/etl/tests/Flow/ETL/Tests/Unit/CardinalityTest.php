<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function str_repeat;
use function strlen;

final class CardinalityTest extends FlowTestCase
{
    public function test_a_bound_survives_only_if_both_sides_have_one(): void
    {
        static::assertSame(30, Cardinality::atMost(10)->merge(Cardinality::exact(20))->atMost);
        static::assertNull(Cardinality::atMost(10)->merge(Cardinality::approximately(20))->atMost);
    }

    public function test_a_negative_bound_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cardinality upper bound must not be negative, given: -1');

        new Cardinality(atMost: -1);
    }

    public function test_a_negative_estimate_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Cardinality estimate must not be negative, given: -1');

        new Cardinality(estimate: -1);
    }

    public function test_a_negative_relative_error_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Relative error must not be negative, given: -0.1');

        new Cardinality(relativeError: -0.1);
    }

    public function test_an_estimate_survives_only_if_both_sides_have_one(): void
    {
        static::assertSame(30, Cardinality::approximately(10)->merge(Cardinality::exact(20))->estimate);
        static::assertNull(Cardinality::approximately(10)->merge(Cardinality::atMost(20))->estimate);
    }

    public function test_approximately_is_an_estimate_without_a_bound(): void
    {
        $cardinality = Cardinality::approximately(16820);

        static::assertNull($cardinality->atMost);
        static::assertSame(16820, $cardinality->estimate);
        static::assertSame(Cardinality::DEFAULT_RELATIVE_ERROR, $cardinality->relativeError);
    }

    public function test_approximately_is_confident_within_its_declared_error(): void
    {
        static::assertSame(16820, Cardinality::approximately(16820, 0.5)->confident(0.5));
    }

    public function test_approximately_is_not_confident_below_its_declared_error(): void
    {
        static::assertNull(Cardinality::approximately(16820, 0.5)->confident(0.1));
    }

    public function test_at_most_is_a_bound_without_an_estimate(): void
    {
        $cardinality = Cardinality::atMost(500);

        static::assertSame(500, $cardinality->atMost);
        static::assertNull($cardinality->estimate);
    }

    public function test_at_most_is_never_confident(): void
    {
        static::assertNull(Cardinality::atMost(500)->confident(1.0));
    }

    public function test_exact_is_both_a_bound_and_an_estimate(): void
    {
        $cardinality = Cardinality::exact(1000);

        static::assertSame(1000, $cardinality->atMost);
        static::assertSame(1000, $cardinality->estimate);
        static::assertSame(0.0, $cardinality->relativeError);
    }

    public function test_exact_is_confident_at_zero_error(): void
    {
        static::assertSame(1000, Cardinality::exact(1000)->confident(0.0));
    }

    public function test_the_sum_carries_the_larger_relative_error(): void
    {
        static::assertSame(
            0.3,
            Cardinality::approximately(10, 0.1)->merge(Cardinality::approximately(20, 0.3))->relativeError,
        );
        static::assertSame(
            0.3,
            Cardinality::approximately(10, 0.3)->merge(Cardinality::approximately(20, 0.1))->relativeError,
        );
    }

    public function test_two_exact_counts_add_up_to_an_exact_count(): void
    {
        static::assertEquals(Cardinality::exact(30), Cardinality::exact(10)->merge(Cardinality::exact(20)));
    }

    public function test_an_exact_count_reads_back_exactly(): void
    {
        static::assertSame(10, Cardinality::exact(10)->exactly());
        static::assertSame(0, Cardinality::exact(0)->exactly());
    }

    public static function inexact_counts(): Generator
    {
        yield 'approximate' => [Cardinality::approximately(10)];
        yield 'approximate with no error, no bound' => [Cardinality::approximately(10, 0.0)];
        yield 'estimate equal to its bound, with an error' => [new Cardinality(10, 10)];
        yield 'estimate under its bound, no error' => [new Cardinality(10, 8, 0.0)];
        yield 'upper bound alone' => [Cardinality::atMost(10)];
        yield 'unknown' => [Cardinality::unknown()];
    }

    #[DataProvider('inexact_counts')]
    public function test_a_count_that_is_not_exact_reads_back_as_null(Cardinality $cardinality): void
    {
        static::assertNull($cardinality->exactly());
    }

    public function test_only_a_count_with_neither_bound_nor_estimate_is_unknown(): void
    {
        static::assertTrue(Cardinality::unknown()->isUnknown());
        static::assertFalse(Cardinality::atMost(10)->isUnknown());
        static::assertFalse(Cardinality::approximately(10)->isUnknown());
        static::assertFalse(Cardinality::exact(0)->isUnknown());
    }

    public function test_unknown_knows_nothing(): void
    {
        $cardinality = Cardinality::unknown();

        static::assertNull($cardinality->atMost);
        static::assertNull($cardinality->estimate);
        static::assertNull($cardinality->confident(1.0));
    }

    public function test_unknown_merged_with_anything_is_unknown(): void
    {
        static::assertEquals(Cardinality::unknown(), Cardinality::unknown()->merge(Cardinality::exact(10)));
        static::assertEquals(Cardinality::unknown(), Cardinality::exact(10)->merge(Cardinality::unknown()));
    }

    public function test_a_validated_count_reads_back_as_a_non_negative_int(): void
    {
        // str_repeat() takes int<0, max>: the analyzer accepts these properties only because the guard narrowed them
        static::assertSame(5, strlen(str_repeat('x', Cardinality::atMost(5)->atMost ?? 0)));
        static::assertSame(7, strlen(str_repeat('x', Cardinality::approximately(7)->estimate ?? 0)));
    }
}
