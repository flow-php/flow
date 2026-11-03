<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Pipe;

use Flow\ETL\Storage\Memory\ArrayMemory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\aggregate;
use function Flow\ETL\DSL\count_rows;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\fetch;
use function Flow\ETL\DSL\filter;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\group_by;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\read;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\run;
use function Flow\ETL\DSL\select;
use function Flow\ETL\DSL\sort_by;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_memory;
use function Flow\ETL\DSL\with_column;
use function Flow\ETL\DSL\write;

final class PipeOperatorTest extends FlowTestCase
{
    public function test_pipeline_from_read_to_fetch(): void
    {
        $rows = [['a' => 1, 'b' => 1], ['a' => 2, 'b' => 3], ['a' => 4, 'b' => 1]];
        $expected = [['a' => 4, 'b' => 1, 'total' => 5], ['a' => 2, 'b' => 3, 'total' => 5]];

        static::assertSame(
            $expected,
            (df()
                |> read(from_array($rows))
                |> with_column('total', ref('a')->plus(ref('b')))
                |> filter(ref('total')->greaterThan(lit(3)))
                |> sort_by(ref('a')->desc())
                |> fetch())->toArray(),
        );
        static::assertSame(
            $expected,
            df()
                ->read(from_array($rows))
                ->withColumn('total', ref('a')->plus(ref('b')))
                ->filter(ref('total')->greaterThan(lit(3)))
                ->sortBy(ref('a')->desc())
                ->fetch()
                ->toArray(),
        );
    }

    public function test_group_by_and_aggregate(): void
    {
        static::assertSame(
            [['country' => 'PL', 'total_sum' => 3.0], ['country' => 'US', 'total_sum' => 3.0]],
            (df()
                |> read(from_array([
                    ['country' => 'PL', 'total' => 1],
                    ['country' => 'PL', 'total' => 2],
                    ['country' => 'US', 'total' => 3],
                ]))
                |> group_by('country')
                |> aggregate([sum(ref('total'))])
                |> fetch())->toArray(),
        );
    }

    public function test_write_and_run(): void
    {
        $memory = new ArrayMemory();

        df() |> read(from_array([['id' => 1], ['id' => 2]])) |> write(to_memory($memory)) |> run();

        static::assertSame([['id' => 1], ['id' => 2]], $memory->dump());
    }

    public function test_count_rows(): void
    {
        static::assertSame(2, df() |> read(from_array([['id' => 1], ['id' => 2]])) |> count_rows());
    }

    public function test_existing_transformation_and_with_column_object(): void
    {
        static::assertSame(
            [['id' => 1, 'x' => 1], ['id' => 2, 'x' => 1]],
            (df()
                |> read(from_array([['id' => 1, 'name' => 'a'], ['id' => 2, 'name' => 'b']]))
                |> select('id')
                |> with_column(int_schema('x'), lit(1))
                |> fetch())->toArray(),
        );
    }
}
