<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\DataFrame;

use Flow\ETL\ErrorHandler\IgnoreError;
use Flow\ETL\Memory\ArrayMemory;
use Flow\ETL\Optimizer;
use Flow\ETL\Tests\Double\RecordingFileExtractor;
use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\PartitionedSourceMother;
use Flow\ETL\Transformation\AddRowIndex\StartFrom;
use Flow\ETL\Transformer\AddRowIndexTransformer;
use Flow\Filesystem\Path\Filter;
use RuntimeException;

use function array_filter;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_all;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\from_data_frame;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\join_on;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\to_memory;

final class SinglePlanTest extends FlowTestCase
{
    public function test_a_sink_inside_a_joined_frame_writes_every_row_of_that_frame(): void
    {
        $memory = new ArrayMemory();
        $right = df()->read(from_array([['id' => 1, 'n' => 'a'], ['id' => 2, 'n' => 'b']]))->write(to_memory($memory));

        $rows = df()
            ->read(from_array([['id' => 1]]))
            ->join($right, join_on(['id' => 'id'], 'r_'))
            ->fetch();

        static::assertSame(1, $rows->count());
        static::assertCount(2, $memory->dump());
    }

    public function test_a_read_frame_wrapped_in_another_extractor_still_reads(): void
    {
        $a = df()->read(from_array([['id' => 1]]));
        $b = df()->read(from_array([['id' => 2]]));

        static::assertSame(
            [['id' => 1], ['id' => 2]],
            df()
                ->read(from_all(from_data_frame($a), from_data_frame($b)))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_declared_schema_re_types_the_read_frames_rows(): void
    {
        $inner = df()->read(from_array([['id' => '1']]));

        static::assertSame(
            [['id' => 1]],
            df()
                ->read(from_data_frame($inner)->withSchema(schema(int_schema('id'))))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_sink_inside_a_read_frame_receives_every_row_under_an_outer_limit(): void
    {
        $memory = new ArrayMemory();
        $inner = df()->read(from_array([['id' => 1], ['id' => 2], ['id' => 3]]))->write(to_memory($memory));

        $rows = df()->read(from_data_frame($inner))->limit(1)->fetch();

        static::assertSame(1, $rows->count());
        static::assertCount(3, $memory->dump());
    }

    public function test_a_joined_frame_is_optimized_with_the_outer_frames_optimizer(): void
    {
        $source = new RecordingFileExtractor(
            schema(int_schema('id')),
            rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 2])),
        );
        $right = df(config_builder()->optimizer(new Optimizer()))->read($source)->limit(1);

        df()
            ->read(from_array([['id' => 1]]))
            ->join($right, join_on(['id' => 'id'], 'r_'))
            ->fetch();

        static::assertSame([1], $source->limits);
    }

    public function test_a_joined_frames_own_error_handler_is_ignored(): void
    {
        $right = df()
            ->read(from_array([['id' => 1]]))
            ->transform(new ThrowingTransformer(new RuntimeException('right boom')))
            ->onError(new IgnoreError());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('right boom');

        df()
            ->read(from_array([['id' => 1]]))
            ->join($right, join_on(['id' => 'id'], 'r_'))
            ->fetch();
    }

    public function test_a_stateful_step_both_sides_of_a_self_join_share_runs_once_per_side(): void
    {
        $frame = df()
            ->read(from_array([['id' => 1], ['id' => 2]]))
            ->transform(new AddRowIndexTransformer('index', StartFrom::ZERO));

        static::assertSame(
            [
                ['id' => 1, 'index' => 0, 'r_id' => 1, 'r_index' => 0],
                ['id' => 2, 'index' => 1, 'r_id' => 2, 'r_index' => 1],
            ],
            $frame
                ->join($frame, join_on(['index' => 'index'], 'r_'))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_a_filter_pushed_into_a_source_both_sides_share_leaves_the_joined_side_unfiltered(): void
    {
        $source = PartitionedSourceMother::yearMonth();
        $base = df()->read($source);
        $joined = df()->read(from_array([['k' => 1]]))->crossJoin($base, 'b_');

        $base
            ->filter(ref('year')->equals(lit(2023)))
            ->crossJoin($joined, 'j_')
            ->fetch();

        $accepts2024 = static fn(Filter $filter): bool => $filter->accept(PartitionedSourceMother::file(
            'year=2024/month=08',
        ));

        static::assertCount(2, $source->pathFilters);
        static::assertCount(1, array_filter($source->pathFilters, $accepts2024));
    }
}
