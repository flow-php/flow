<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\ProgressUnit;
use Flow\ETL\Foundation\StageName;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\HeartbeatContext;
use Flow\ETL\Tests\Double\ProgressSpy;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Rows;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class MemorySortProcessorTest extends FlowTestCase
{
    public function test_a_sort_is_ordered_by_its_columns(): void
    {
        $columns = [ref('id')->desc()];

        static::assertSame($columns, (new MemorySortProcessor($columns))->distribution()?->order);
    }

    public function test_bind_returns_the_input_schema_and_declares_it_on_the_rebound_step(): void
    {
        $columns = [ref('id')->asc()];
        $input = schema(int_schema('id'));
        $bound = (new MemorySortProcessor($columns))->bind($input);

        static::assertEquals($input, $bound->output);
        static::assertEquals(new MemorySortProcessor($columns, $input), $bound->step);
    }

    /**
     * A buffer with no rows yields no batch at all, so the declared schema is observable only on a
     * batch that carries rows - there it still wins over the schema the batch arrived with.
     */
    public function test_the_declared_schema_wins_over_the_batch_schema(): void
    {
        $declared = schema(int_schema('id', nullable: true));

        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()], $declared))->process(
            (static function () {
                yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));

        static::assertCount(1, $result);
        static::assertEquals($declared, $result[0]->schema());
    }

    public function test_handles_empty_input(): void
    {
        static::assertCount(
            0,
            iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
                (static function () {
                    yield from [];
                })(),
                flow_context()->backend(),
            )),
        );
    }

    public function test_sorts_across_multiple_batches(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                yield array_to_rows([['id' => 1]], schema(int_schema('id')));
                yield array_to_rows([['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_ascending(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->asc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 3], ['id' => 1], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_descending(): void
    {
        /** @var list<Rows> $result */
        $result = iterator_to_array((new MemorySortProcessor([ref('id')->desc()]))->process(
            (static function () {
                yield array_to_rows([['id' => 1], ['id' => 3], ['id' => 2]], schema(int_schema('id')));
            })(),
            flow_context()->backend(),
        ));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 3], ['id' => 2], ['id' => 1]], $allRows);
    }

    public function test_rebuilds_with_the_rebuild_progress(): void
    {
        $spy = new ProgressSpy();

        iterator_to_array(
            DescribeRoundTrip::rebuilt(new MemorySortProcessor([ref('id')->asc()]), new Rebuild($spy))->process(
                (static function () {
                    yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(['sort'], $spy->stages());
        static::assertGreaterThan(0, $spy->countOf(StageName::sort));
    }

    public function test_a_bound_sort_counts_with_its_progress(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new MemorySortProcessor([ref('id')->asc()], progress: $progress->stage(StageName::sort)))
                ->bind(schema(int_schema('id')))
                ->step->process(
                    (static function () {
                        yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
                    })(),
                    flow_context()->backend(),
                ),
            false,
        );

        static::assertSame(2, $progress->countOf(StageName::sort));
    }

    public function test_counts_every_row_it_takes_once_and_is_busy_while_it_emits(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new MemorySortProcessor([ref('id')->asc()], progress: $progress->stage(StageName::sort)))->process(
                (static function () {
                    yield array_to_rows([['id' => 2], ['id' => 1]], schema(int_schema('id')));
                    yield array_to_rows([['id' => 3]], schema(int_schema('id')));
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame([3, 2], [$progress->countOf(StageName::sort), $progress->busyCalls(StageName::sort)]);
    }

    public function test_memory_sort_processor_describes_its_progress(): void
    {
        $spy = new ProgressSpy();

        DescribeRoundTrip::rebuilt(
            new MemorySortProcessor([ref('id')->asc()], progress: HeartbeatContext::stageWithId(StageName::sort, 7)),
            new Rebuild($spy),
        );

        static::assertSame([['sort', ProgressUnit::rows, 7, null]], $spy->built());
    }

    public function test_memory_sort_processor_is_done_after_its_last_batch(): void
    {
        $progress = new ProgressSpy();
        $output = (new MemorySortProcessor([ref('id')->asc()], progress: $progress->stage(StageName::sort)))->process(
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
            (new MemorySortProcessor([ref('id')->desc()], progress: $stage))->distribution()?->progress,
        );
    }

    public function test_a_memory_sort_over_an_empty_input_is_done_once(): void
    {
        $progress = new ProgressSpy();

        iterator_to_array(
            (new MemorySortProcessor([ref('id')->asc()], progress: $progress->stage(StageName::sort)))->process(
                (static function (): Generator {
                    yield from [];
                })(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertSame(1, $progress->doneCalls(StageName::sort));
    }
}
