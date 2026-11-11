<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Values\Rows;
use Generator;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class MergeSortProcessorTest extends FlowTestCase
{
    public function test_a_sort_is_ordered_by_its_columns(): void
    {
        $columns = [ref('id')->desc()];

        static::assertSame(
            $columns,
            (new MergeSortProcessor(
                new ExternalSortConfig(config()->grouping),
                $columns,
                new NativePHPRandomValueGenerator(),
            ))->distribution()?->order,
        );
    }

    public function test_a_sort_that_does_not_describe_has_no_distribution(): void
    {
        static::assertNull(
            (new MergeSortProcessor(
                new ExternalSortConfig(config()->grouping),
                [ref('id')->asc()],
                new FixedRandomValueGenerator('fixed'),
            ))->distribution(),
        );
    }

    public function test_a_processor_with_an_undescribable_random_value_generator_does_not_describe(): void
    {
        static::assertNull(
            (new MergeSortProcessor(
                new ExternalSortConfig(config()->grouping),
                [ref('id')->asc()],
                new FixedRandomValueGenerator('fixed'),
            ))->describe(),
        );
    }

    public function test_bind_returns_the_input_schema(): void
    {
        $input = schema(int_schema('id'));

        static::assertEquals(
            $input,
            (new MergeSortProcessor(
                ExternalSortMother::inMemoryConfig(),
                [ref('id')->asc()],
                new NativePHPRandomValueGenerator(),
            ))->bind($input)->output,
        );
    }

    public function test_process_sorts_through_the_external_sort(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());
        static::assertSame(
            [1, 2, 3],
            array_merge(...array_map(
                static fn(Rows $rows): array => $rows->reduceToArray('id'),
                iterator_to_array(
                    (new MergeSortProcessor(
                        ExternalSortMother::spillingConfig($storage),
                        [ref('id')->asc()],
                        new NativePHPRandomValueGenerator(),
                    ))->process(
                        (static function (): Generator {
                            yield array_to_rows([['id' => 3], ['id' => 1]], schema(int_schema('id')));
                            yield array_to_rows([['id' => 2]], schema(int_schema('id')));
                        })(),
                        flow_context(config())->backend(),
                    ),
                    false,
                ),
            )),
        );
        static::assertNotSame([], $storage->appended);
    }

    public function test_an_empty_stream_sorts_to_nothing(): void
    {
        $storage = new RecordingRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            [],
            iterator_to_array(
                (new MergeSortProcessor(
                    ExternalSortMother::spillingConfig($storage),
                    [ref('id')->asc()],
                    new NativePHPRandomValueGenerator(),
                ))->process((static fn(): Generator => yield from [])(), flow_context()->backend()),
                false,
            ),
        );
        static::assertSame([], $storage->appended);
    }

    public function test_rebuilds_with_the_rebuild_progress(): void
    {
        $spy = new ProgressSpy();

        iterator_to_array(
            DescribeRoundTrip::rebuilt(
                new MergeSortProcessor(
                    new ExternalSortConfig(config()->grouping),
                    [ref('id')->asc()],
                    new NativePHPRandomValueGenerator(),
                ),
                new Rebuild($spy),
            )->process(
                (static function (): Generator {
                    yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
                })(),
                flow_context(config())->backend(),
            ),
            false,
        );

        static::assertSame(['sort'], $spy->stages());
        static::assertGreaterThan(0, $spy->countOf(StageName::sort));
    }

    public function test_merge_sort_processor_describes_its_progress(): void
    {
        $spy = new ProgressSpy();

        DescribeRoundTrip::rebuilt(
            new MergeSortProcessor(
                new ExternalSortConfig(config()->grouping),
                [ref('id')->asc()],
                new NativePHPRandomValueGenerator(),
                HeartbeatContext::stageWithId(StageName::sort, 7),
            ),
            new Rebuild($spy),
        );

        static::assertSame([['sort', ProgressUnit::rows, 7, null]], $spy->built());
    }

    public function test_merge_sort_processor_is_done_after_its_last_batch(): void
    {
        $progress = new ProgressSpy();
        $output = (new MergeSortProcessor(
            new ExternalSortConfig(config()->grouping),
            [ref('id')->asc()],
            new NativePHPRandomValueGenerator(),
            $progress->stage(StageName::sort),
        ))->process(
            (static function (): Generator {
                yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
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
            (new MergeSortProcessor(
                new ExternalSortConfig(config()->grouping),
                [ref('id')->desc()],
                new NativePHPRandomValueGenerator(),
                $stage,
            ))->distribution()?->progress,
        );
    }
}
