<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Tests\Context\ScoredRows;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function iterator_to_array;
use function serialize;

final class TopNProcessorTest extends FlowTestCase
{
    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 200; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_under_the_memory_limit_it_trims_in_memory_with_zero_spills(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());
        $refs = refs(ref('score'));

        $top = ScoredRows::merged((new TopNProcessor($refs, 2, ExternalSortMother::inMemory($refs, $storage)))->process(
            ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']]),
            flow_context(),
        ));

        static::assertSame([['score' => 0, 'name' => 'z'], ['score' => 1, 'name' => 'a']], $top);
        static::assertSame([], $storage->appended);
    }

    public function test_past_the_memory_limit_it_falls_back_to_the_spilling_sort(): void
    {
        $storage = new RecordingBucketsStorage(new MemoryBuckets());
        $refs = refs(ref('score'));

        $top = ScoredRows::merged((new TopNProcessor($refs, 2, ExternalSortMother::spilling($refs, $storage)))->process(
            ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']], [[1, 'y']]),
            flow_context(),
        ));

        static::assertSame([['score' => 0, 'name' => 'z'], ['score' => 1, 'name' => 'a']], $top);
        static::assertNotSame([], $storage->appended);
    }

    public function test_a_fallback_on_the_last_batch_emits_the_kept_rows(): void
    {
        $refs = refs(ref('score'));

        static::assertSame(
            [['score' => 1, 'name' => 'a']],
            ScoredRows::merged((new TopNProcessor($refs, 1, ExternalSortMother::spilling($refs)))->process(
                ScoredRows::batches([[3, 'c'], [1, 'a']]),
                flow_context(),
            )),
        );
    }

    #[DataProvider('random_datasets')]
    public function test_the_fallback_equals_a_sort_followed_by_a_limit(int $seed): void
    {
        $dataset = SortDatasetMother::random($seed);
        $limit = ($seed % 7) + 1;
        $refs = refs(...$dataset['references']);
        $input = (static function () use ($dataset): Generator {
            foreach ($dataset['runs'] as $rows) {
                yield array_to_rows($rows, $dataset['schema']);
            }
        })();

        $top = rows($dataset['schema']);
        $storage = new RecordingBucketsStorage(new MemoryBuckets());

        foreach ((new TopNProcessor(
            $refs,
            $limit,
            ExternalSortMother::spilling($refs, $storage, mergeFanIn: 2),
        ))->process($input, flow_context()) as $batch) {
            $top = $top->concat(new AdaptiveBackend(), $batch->matchTo($dataset['schema'], new AdaptiveBackend()));
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                array_to_rows(array_merge(...$dataset['runs']), $dataset['schema'])
                    ->sortBy(...$dataset['references'])
                    ->take($limit)
                    ->toArray(),
            ),
            serialize($top->toArray()),
        );

        if (array_merge(...$dataset['runs']) !== []) {
            static::assertNotSame([], $storage->appended, 'the fallback must have spilled');
        }
    }

    public function test_bind_keeps_the_fallback(): void
    {
        $fallback = ExternalSortMother::inMemory(refs(ref('score')));
        $bound = (new TopNProcessor(refs(ref('score')), 3, $fallback))->bind(schema(int_schema('score')))->step;

        static::assertInstanceOf(TopNProcessor::class, $bound);
        static::assertSame($fallback, $bound->fallback);
    }

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

    public function test_top_sorts_and_keeps_the_limit(): void
    {
        static::assertSame(
            [['score' => 9, 'name' => 'a'], ['score' => 7, 'name' => 'b']],
            (new TopNProcessor(refs(ref('score')->desc()), 2))->top(array_to_rows(
                [['score' => 3, 'name' => 'c'], ['score' => 9, 'name' => 'a'], ['score' => 7, 'name' => 'b']],
                schema(int_schema('score'), str_schema('name')),
            ))->toArray(),
        );
    }

    public function test_batches_are_conformed_to_the_declared_schema(): void
    {
        $declared = schema(int_schema('score', nullable: true), str_schema('name'));
        $bound = (new TopNProcessor(refs(ref('score')), 5))->bind($declared)->step;
        static::assertInstanceOf(TopNProcessor::class, $bound);

        $out = iterator_to_array($bound->process(ScoredRows::batches([[2, 'b'], [1, 'a']]), flow_context()), false);

        static::assertEquals($declared, $out[0]->schema());
        static::assertSame([['score' => 1, 'name' => 'a'], ['score' => 2, 'name' => 'b']], $out[0]->toArray());
    }
}
