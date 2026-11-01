<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\SequenceGenerator;

use Flow\ETL\IO\Extractor\SequenceGenerator\NumberSequenceGenerator;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Cardinality;
use PHPUnit\Framework\Attributes\TestWith;
use ValueError;

use function iterator_count;
use function iterator_to_array;
use function range;

use const PHP_INT_MAX;

final class NumberSequenceGeneratorTest extends FlowTestCase
{
    #[TestWith([1, 10, 1, 10])]
    #[TestWith([1, 10, 3, 4])]
    #[TestWith([10, 1, 1, 10])]
    #[TestWith([5, 1, 2, 3])]
    #[TestWith([0, 1, 0.3, 4])]
    #[TestWith([0, 1, 0.4, 3])]
    #[TestWith([0.1, 0.7, 0.1, 6])]
    #[TestWith([7, 7, 1, 1])]
    #[TestWith([-500, 500, 7, 143])]
    #[TestWith([10, 1, -3, 4])]
    #[TestWith([-35, 97, 2.2, 61])]
    #[TestWith([92.3, -5.7, 1, 98])]
    #[TestWith(['1', '10', 3, 4])]
    #[TestWith(['a', 'e', 1, 5])]
    public function test_rows_count_exactly_what_generate_yields(
        string|int|float $start,
        string|int|float $end,
        int|float $step,
        int $expected,
    ): void {
        $generator = new NumberSequenceGenerator($start, $end, $step);

        static::assertEquals(Cardinality::exact($expected), $generator->rows());
        static::assertSame($expected, iterator_count($generator->generate()));
    }

    #[TestWith([1, 4, 1, [1, 2, 3, 4]])]
    #[TestWith([1, 10, 4, [1, 5, 9]])]
    #[TestWith([4, 1, 1, [4, 3, 2, 1]])]
    #[TestWith([10, 1, 4, [10, 6, 2]])]
    #[TestWith([-2, 2, 2, [-2, 0, 2]])]
    #[TestWith([10, 1, -4, [10, 6, 2]])]
    #[TestWith([7, 7, 1, [7]])]
    #[TestWith([0, 1, 0.5, [0.0, 0.5, 1.0]])]
    public function test_generate_yields_the_values_range_yields(
        int $start,
        int $end,
        int|float $step,
        array $expected,
    ): void {
        static::assertSame($expected, iterator_to_array(
            (new NumberSequenceGenerator($start, $end, $step))->generate(),
            false,
        ));
        static::assertSame(range($start, $end, $step), $expected);
    }

    public function test_an_integer_sequence_is_generated_without_building_it_first(): void
    {
        $generator = (new NumberSequenceGenerator(0, PHP_INT_MAX))->generate();

        static::assertSame(0, $generator->current());
        $generator->next();
        static::assertSame(1, $generator->current());
    }

    #[TestWith([1, 10, 0])]
    #[TestWith([1, 3, 5])]
    #[TestWith([1, 10, -2])]
    public function test_a_step_the_range_rejects_is_rejected_the_same_way(int $start, int $end, int $step): void
    {
        $this->expectException(ValueError::class);

        (new NumberSequenceGenerator($start, $end, $step))->rows();
    }
}
