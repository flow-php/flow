<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Tests\Context\ScoredRows;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;

final class TopNProcessorTest extends FlowTestCase
{
    public function test_it_emits_exactly_what_a_sort_followed_by_a_limit_emits(): void
    {
        $refs = refs(ref('score')->desc(), ref('name'));
        $batches = static fn(): Generator => ScoredRows::batches(
            [[3, 'c'], [9, 'a'], [3, 'b']],
            [[7, 'x'], [9, 'z'], [1, 'q']],
            [[9, 'b'], [3, 'a'], [7, 'y']],
        );

        $sorted = ScoredRows::merged((new MemorySortProcessor($refs))->process($batches(), flow_context()));
        $top = ScoredRows::merged((new TopNProcessor($refs, 4))->process($batches(), flow_context()));

        static::assertSame(array_slice($sorted, 0, 4), $top);
    }

    public function test_ties_keep_the_order_they_arrived_in(): void
    {
        $top = ScoredRows::merged((new TopNProcessor(refs(ref('score')), 3))->process(
            ScoredRows::batches([[1, 'first'], [1, 'second']], [[1, 'third'], [1, 'fourth']]),
            flow_context(),
        ));

        static::assertSame(['first', 'second', 'third'], array_column($top, 'name'));
    }

    public function test_it_keeps_every_row_when_the_input_is_shorter_than_the_limit(): void
    {
        $top = ScoredRows::merged((new TopNProcessor(refs(ref('score')), 10))->process(ScoredRows::batches([
            [2, 'b'],
            [1, 'a'],
        ]), flow_context()));

        static::assertSame([['score' => 1, 'name' => 'a'], ['score' => 2, 'name' => 'b']], $top);
    }

    public function test_it_trims_while_reading_many_batches(): void
    {
        $input = [];

        for ($i = 100; $i > 0; $i--) {
            $input[] = [[$i, 'n' . $i]];
        }

        $top = ScoredRows::merged((new TopNProcessor(refs(ref('score')), 2))->process(
            ScoredRows::batches(...$input),
            flow_context(),
        ));

        static::assertSame([1, 2], array_column($top, 'score'));
    }

    public function test_an_empty_input_emits_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array((new TopNProcessor(refs(ref('score')), 3))->process(
                ScoredRows::batches(),
                flow_context(),
            )),
        );
    }

    public function test_bind_keeps_the_input_schema_as_output(): void
    {
        $schema = schema(int_schema('score'), str_schema('name'));

        static::assertSame($schema, (new TopNProcessor(refs(ref('score')), 3))->bind($schema)->output);
    }

    public function test_a_limit_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TopN limit must be greater than 0, given: 0');

        new TopNProcessor(refs(ref('score')), 0);
    }
}
