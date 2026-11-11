<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Context\ScoredRows;
use Flow\ETL\Tests\Context\SortedRows;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\SortDatasetMother;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function array_values;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function ini_get;
use function ini_set;
use function iterator_to_array;
use function serialize;

final class TopNProcessorTest extends FlowTestCase
{
    public function test_a_top_n_meets_in_one_process_after_every_split_keeps_its_own_top(): void
    {
        $topN = new TopNProcessor([ref('id')->desc()], 2, new MemorySortConfig(), new NativePHPRandomValueGenerator());
        $distribution = $topN->distribution();

        static::assertSame([], $distribution?->keys);
        static::assertSame($topN, $distribution->partial);
        static::assertNull($distribution->final);
    }

    public function test_a_top_n_that_does_not_describe_has_no_distribution(): void
    {
        static::assertNull(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new ExternalSortConfig(config()->grouping),
                new FixedRandomValueGenerator('fixed'),
            ))->distribution(),
        );
    }

    public function test_a_processor_with_an_undescribable_random_value_generator_does_not_describe(): void
    {
        static::assertNull(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new ExternalSortConfig(config()->grouping),
                new FixedRandomValueGenerator('fixed'),
            ))->describe(),
        );
    }

    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 200; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_under_the_memory_limit_it_trims_in_memory_with_zero_spills(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        static::assertSame(
            [['score' => 0, 'name' => 'z'], ['score' => 1, 'name' => 'a']],
            ScoredRows::merged((new TopNProcessor(
                [ref('score')->asc()],
                2,
                ExternalSortMother::inMemoryConfig($storage),
                new NativePHPRandomValueGenerator(),
            ))->process(ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']]), flow_context()->backend())),
        );
        static::assertSame([], $storage->appended);
    }

    public function test_past_the_memory_limit_it_falls_back_to_the_spilling_sort(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        static::assertSame(
            [['score' => 0, 'name' => 'z'], ['score' => 1, 'name' => 'a']],
            ScoredRows::merged((new TopNProcessor(
                [ref('score')->asc()],
                2,
                ExternalSortMother::spillingConfig($storage),
                new NativePHPRandomValueGenerator(),
            ))->process(
                ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']], [[1, 'y']]),
                flow_context()->backend(),
            )),
        );
        static::assertNotSame([], $storage->appended);
    }

    public function test_a_fallback_counts_every_row_it_takes_once(): void
    {
        $progress = new ProgressSpy();

        ScoredRows::merged((new TopNProcessor(
            [ref('score')->asc()],
            2,
            ExternalSortMother::spillingConfig(),
            new NativePHPRandomValueGenerator(),
            progress: $progress->stage(StageName::sort),
        ))->process(
            ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']], [[1, 'y']]),
            flow_context()->backend(),
        ));

        static::assertSame(5, $progress->countOf(StageName::sort));
    }

    public function test_fallback_sort_counts_the_rows_top_n_already_holds(): void
    {
        $memoryLimit = (string) ini_get('memory_limit');
        ini_set('memory_limit', '-1');

        try {
            $storage = new SpyRowsStorage(new MemoryRowsStorage());
            $backend = new ReportedMemoryBackend(0);
            $schema = schema(int_schema('score'), str_schema('name'));

            static::assertSame(
                [['score' => 0, 'name' => 'z'], ['score' => 1, 'name' => 'a']],
                ScoredRows::merged((new TopNProcessor(
                    [ref('score')->asc()],
                    2,
                    ExternalSortMother::config($storage, Unit::fromBytes(1_000_000_000)),
                    new NativePHPRandomValueGenerator(),
                ))->process(
                    (static function () use ($backend, $schema): Generator {
                        // every batch is held for the rest of the run
                        foreach ([[[5, 'e'], [4, 'd']], [[3, 'c'], [2, 'b']], [[1, 'a'], [0, 'z']]] as $batch) {
                            $backend->reported += 600_000_000;

                            yield array_to_rows(array_map(static fn(array $row): array => [
                                'score' => $row[0],
                                'name' => $row[1],
                            ], $batch), $schema);
                        }
                    })(),
                    $backend,
                )),
            );
            static::assertSame(
                [2, 3],
                array_values($storage->appendedRows())[0][0]->reduceToArray('score'),
                'the kept rows are the first run: the sort already counts the memory top-n grew',
            );
        } finally {
            ini_set('memory_limit', $memoryLimit);
        }
    }

    public function test_a_fallback_on_the_last_batch_emits_the_kept_rows(): void
    {
        static::assertSame(
            [['score' => 1, 'name' => 'a']],
            ScoredRows::merged((new TopNProcessor(
                [ref('score')->asc()],
                1,
                ExternalSortMother::spillingConfig(),
                new NativePHPRandomValueGenerator(),
            ))->process(ScoredRows::batches([[3, 'c'], [1, 'a']]), flow_context()->backend())),
        );
    }

    #[DataProvider('random_datasets')]
    public function test_the_fallback_equals_a_sort_followed_by_a_limit(int $seed): void
    {
        $dataset = SortDatasetMother::random($seed);
        $limit = ($seed % 7) + 1;
        $input = (static function () use ($dataset): Generator {
            foreach ($dataset['runs'] as $rows) {
                yield array_to_rows($rows, $dataset['schema']);
            }
        })();

        $top = rows($dataset['schema']);
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());

        foreach ((new TopNProcessor(
            $dataset['columns'],
            $limit,
            ExternalSortMother::spillingConfig($storage, mergeFanIn: 2),
            new NativePHPRandomValueGenerator(),
        ))->process($input, flow_context()->backend()) as $batch) {
            $top = Rows::concat(
                $top->schema(),
                new AdaptiveBackend(),
                $top,
                $batch->matchTo($dataset['schema'], new AdaptiveBackend()),
            );
        }

        // serialized, so NaN and -0.0 compare by what they are
        static::assertSame(
            serialize(
                SortedRows::of(
                    array_to_rows(array_merge(...$dataset['runs']), $dataset['schema']),
                    ...$dataset['columns'],
                )->take($limit)->toArray(),
            ),
            serialize($top->toArray()),
        );

        if (array_merge(...$dataset['runs']) !== []) {
            static::assertNotSame([], $storage->appended, 'the fallback must have spilled');
        }
    }

    public function test_bind_keeps_the_config(): void
    {
        $config = ExternalSortMother::inMemoryConfig();
        $bound = (new TopNProcessor(
            [ref('score')->asc()],
            3,
            $config,
            new NativePHPRandomValueGenerator(),
        ))->bind(schema(int_schema('score')))->step;

        static::assertInstanceOf(TopNProcessor::class, $bound);
        static::assertSame($config, $bound->config);
        static::assertNotNull($bound->fallback);
    }

    public function test_it_emits_exactly_what_a_sort_followed_by_a_limit_emits(): void
    {
        $columns = [ref('score')->desc(), ref('name')->asc()];
        $batches = static fn(): Generator => ScoredRows::batches(
            [[3, 'c'], [9, 'a'], [3, 'b']],
            [[7, 'x'], [9, 'z'], [1, 'q']],
            [[9, 'b'], [3, 'a'], [7, 'y']],
        );

        static::assertSame(
            array_slice(
                ScoredRows::merged((new MemorySortProcessor($columns))->process($batches(), flow_context()->backend())),
                0,
                4,
            ),
            ScoredRows::merged((new TopNProcessor(
                $columns,
                4,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
            ))->process($batches(), flow_context()->backend())),
        );
    }

    public function test_ties_keep_the_order_they_arrived_in(): void
    {
        static::assertSame(
            ['first', 'second', 'third'],
            array_column(
                ScoredRows::merged((new TopNProcessor(
                    [ref('score')->asc()],
                    3,
                    new MemorySortConfig(),
                    new NativePHPRandomValueGenerator(),
                ))->process(
                    ScoredRows::batches([[1, 'first'], [1, 'second']], [[1, 'third'], [1, 'fourth']]),
                    flow_context()->backend(),
                )),
                'name',
            ),
        );
    }

    public function test_it_keeps_every_row_when_the_input_is_shorter_than_the_limit(): void
    {
        static::assertSame(
            [['score' => 1, 'name' => 'a'], ['score' => 2, 'name' => 'b']],
            ScoredRows::merged((new TopNProcessor(
                [ref('score')->asc()],
                10,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
            ))->process(ScoredRows::batches([
                [2, 'b'],
                [1, 'a'],
            ]), flow_context()->backend())),
        );
    }

    public function test_it_trims_while_reading_many_batches(): void
    {
        $input = [];

        for ($i = 100; $i > 0; $i--) {
            $input[] = [[$i, 'n' . $i]];
        }

        static::assertSame(
            [1, 2],
            array_column(
                ScoredRows::merged((new TopNProcessor(
                    [ref('score')->asc()],
                    2,
                    new MemorySortConfig(),
                    new NativePHPRandomValueGenerator(),
                ))->process(ScoredRows::batches(...$input), flow_context()->backend())),
                'score',
            ),
        );
    }

    public function test_an_empty_input_emits_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array((new TopNProcessor(
                [ref('score')->asc()],
                3,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
            ))->process(ScoredRows::batches(), flow_context()->backend())),
        );
    }

    public function test_an_empty_input_with_a_spilling_fallback_emits_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array((new TopNProcessor(
                [ref('score')->asc()],
                3,
                ExternalSortMother::spillingConfig(),
                new NativePHPRandomValueGenerator(),
            ))->process(ScoredRows::batches(), flow_context()->backend())),
        );
    }

    public function test_bind_keeps_the_input_schema_as_output(): void
    {
        $schema = schema(int_schema('score'), str_schema('name'));

        static::assertSame(
            $schema,
            (new TopNProcessor(
                [ref('score')->asc()],
                3,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
            ))->bind($schema)->output,
        );
    }

    public function test_a_limit_below_one_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('TopN limit must be greater than 0, given: 0');

        new TopNProcessor([ref('score')->asc()], 0, new MemorySortConfig(), new NativePHPRandomValueGenerator());
    }

    public function test_top_sorts_and_keeps_the_limit(): void
    {
        static::assertSame(
            [['score' => 9, 'name' => 'a'], ['score' => 7, 'name' => 'b']],
            (new TopNProcessor(
                [ref('score')->desc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
            ))->top(
                array_to_rows(
                    [['score' => 3, 'name' => 'c'], ['score' => 9, 'name' => 'a'], ['score' => 7, 'name' => 'b']],
                    schema(int_schema('score'), str_schema('name')),
                ),
                flow_context()->backend(),
            )->toArray(),
        );
    }

    public function test_batches_are_conformed_to_the_declared_schema(): void
    {
        $declared = schema(int_schema('score', nullable: true), str_schema('name'));
        $bound = (new TopNProcessor(
            [ref('score')->asc()],
            5,
            new MemorySortConfig(),
            new NativePHPRandomValueGenerator(),
        ))->bind($declared)->step;
        static::assertInstanceOf(TopNProcessor::class, $bound);

        $out = iterator_to_array(
            $bound->process(ScoredRows::batches([[2, 'b'], [1, 'a']]), flow_context()->backend()),
            false,
        );

        static::assertEquals($declared, $out[0]->schema());
        static::assertSame([['score' => 1, 'name' => 'a'], ['score' => 2, 'name' => 'b']], $out[0]->toArray());
    }

    public function test_rebuilds_with_the_rebuild_progress(): void
    {
        $spy = new ProgressSpy();

        iterator_to_array(
            DescribeRoundTrip::rebuilt(
                new TopNProcessor(
                    [ref('id')->asc()],
                    2,
                    new ExternalSortConfig(config()->grouping),
                    new NativePHPRandomValueGenerator(),
                ),
                new Rebuild($spy),
            )->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(['sort'], $spy->stages());
        static::assertGreaterThan(0, $spy->countOf(StageName::sort));
    }

    public function test_a_bound_top_n_counts_with_its_progress(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
                progress: $progress->stage(StageName::sort),
            ))
                ->bind(schema(int_schema('id')))
                ->step->process(
                    (static function (): Generator {
                        yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
                    })(),
                    flow_context()->backend(),
                ),
            false,
        );

        static::assertSame(3, $progress->countOf(StageName::sort));
    }

    public function test_counts_every_row_it_takes_once(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
                progress: $progress->stage(StageName::sort),
            ))->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 4], ['id' => 3]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(4, $progress->countOf(StageName::sort));
    }

    public function test_top_n_processor_describes_its_progress(): void
    {
        $spy = new ProgressSpy();

        DescribeRoundTrip::rebuilt(
            new TopNProcessor(
                [ref('id')->asc()],
                2,
                new ExternalSortConfig(config()->grouping),
                new NativePHPRandomValueGenerator(),
                progress: HeartbeatContext::stageWithId(StageName::sort, 7),
            ),
            new Rebuild($spy),
        );

        static::assertSame([['sort', ProgressUnit::rows, 7, null]], $spy->built());
    }

    public function test_top_n_processor_is_done_after_its_last_batch(): void
    {
        $progress = new ProgressSpy();
        $output = (new TopNProcessor(
            [ref('id')->asc()],
            2,
            new MemorySortConfig(),
            new NativePHPRandomValueGenerator(),
            progress: $progress->stage(StageName::sort),
        ))->process(
            (static function (): Generator {
                yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        );

        $output->current();

        static::assertSame(0, $progress->doneCalls(StageName::sort));

        iterator_to_array($output, false);

        static::assertSame(1, $progress->doneCalls(StageName::sort));
    }

    public function test_its_distribution_holds_the_stage_it_reports_to(): void
    {
        $stage = new ProgressSpy(StageName::sort);

        static::assertSame(
            $stage,
            (new TopNProcessor(
                [ref('id')->desc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
                progress: $stage,
            ))->distribution()?->progress,
        );
    }

    public function test_a_top_n_that_falls_back_to_the_spilling_sort_is_done_once(): void
    {
        $progress = new ProgressSpy();

        ScoredRows::merged((new TopNProcessor(
            [ref('score')->asc()],
            2,
            ExternalSortMother::spillingConfig(),
            new NativePHPRandomValueGenerator(),
            progress: $progress->stage(StageName::sort),
        ))->process(
            ScoredRows::batches([[3, 'c'], [1, 'a']], [[2, 'b'], [0, 'z']], [[1, 'y']]),
            flow_context()->backend(),
        ));

        static::assertSame(1, $progress->doneCalls(StageName::sort));
    }

    public function test_a_top_n_over_an_empty_input_is_done_once(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
                progress: $progress->stage(StageName::sort),
            ))->process(
                (static function (): Generator {
                    yield from [];
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(1, $progress->doneCalls(StageName::sort));
    }

    public function test_a_top_n_is_busy_for_each_batch_it_emits(): void
    {
        $progress = new ProgressSpy();

        $emitted = iterator_to_array(
            (new TopNProcessor(
                [ref('id')->asc()],
                2,
                new MemorySortConfig(),
                new NativePHPRandomValueGenerator(),
                progress: $progress->stage(StageName::sort),
            ))->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(count($emitted), $progress->busyCalls(StageName::sort));
    }
}
