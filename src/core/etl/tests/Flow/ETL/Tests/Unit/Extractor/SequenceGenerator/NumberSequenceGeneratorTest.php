<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor\SequenceGenerator;

use Flow\ETL\Cardinality;
use Flow\ETL\Extractor\SequenceGenerator\NumberSequenceGenerator;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;
use ValueError;

use function iterator_count;

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

    #[TestWith([1, 10, 0])]
    #[TestWith([1, 3, 5])]
    #[TestWith([1, 10, -2])]
    public function test_a_step_the_range_rejects_is_rejected_the_same_way(int $start, int $end, int $step): void
    {
        $this->expectException(ValueError::class);

        (new NumberSequenceGenerator($start, $end, $step))->rows();
    }
}
