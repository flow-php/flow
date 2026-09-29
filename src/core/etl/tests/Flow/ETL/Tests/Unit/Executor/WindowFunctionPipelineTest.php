<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Executor;

use Flow\ETL\Executor\Segments;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Tests\Context\ExecutedSegments;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function iterator_to_array;

final class WindowFunctionPipelineTest extends TestCase
{
    public function test_handles_empty_input(): void
    {
        $segments = new Segments(from_rows(rows(schema(int_schema('id')))));

        $window = window()->orderBy(ref('id'));
        $segments->add(new WindowProcessor('row_num', row_number()->over($window)));

        $result = iterator_to_array(ExecutedSegments::of($segments, flow_context(config())));

        static::assertCount(0, $result);
    }

    public function test_handles_no_order_by(): void
    {
        $segments = new Segments(from_rows(array_to_rows(
            [['dept' => 'IT', 'value' => 100], ['dept' => 'IT', 'value' => 150]],
            schema(str_schema('dept'), int_schema('value')),
        )));

        $window = window()->partitionBy(ref('dept'));

        $segments->add(new WindowProcessor('total', sum(ref('value'))->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array(ExecutedSegments::of($segments, $context));

        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);

        static::assertEquals(250, $result[0]->column('total')->value(0));
        static::assertEquals(250, $result[0]->column('total')->value(1));
    }

    public function test_handles_single_row_partition(): void
    {
        // the shuffle is upstream of this processor now, so the test hands it what a shuffle produces:
        // one Rows per partition key
        $schema = schema(str_schema('dept'), int_schema('salary'));
        $segments = new Segments(from_rows(
            array_to_rows([['dept' => 'IT', 'salary' => 5000]], $schema),
            array_to_rows([['dept' => 'HR', 'salary' => 4000]], $schema),
        ));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $segments->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array(ExecutedSegments::of($segments, $context));

        static::assertCount(2, $result);
        static::assertCount(1, $result[0]);
        static::assertCount(1, $result[1]);
    }

    public function test_processes_multiple_partitions_separately(): void
    {
        $schema = schema(str_schema('dept'), int_schema('salary'));
        $segments = new Segments(from_rows(
            array_to_rows([['dept' => 'IT', 'salary' => 5000], ['dept' => 'IT', 'salary' => 6000]], $schema),
            array_to_rows([['dept' => 'HR', 'salary' => 4000], ['dept' => 'HR', 'salary' => 4500]], $schema),
        ));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $segments->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array(ExecutedSegments::of($segments, $context));

        static::assertCount(2, $result);

        static::assertCount(2, $result[0]);
        static::assertEquals(1, $result[0]->column('row_num')->value(0));
        static::assertEquals(2, $result[0]->column('row_num')->value(1));

        static::assertCount(2, $result[1]);
        static::assertEquals(1, $result[1]->column('row_num')->value(0));
        static::assertEquals(2, $result[1]->column('row_num')->value(1));
    }

    public function test_processes_single_partition_without_partition_by(): void
    {
        $segments = new Segments(from_rows(array_to_rows(
            [['id' => 1, 'value' => 100], ['id' => 2, 'value' => 150], ['id' => 3, 'value' => 200]],
            schema(int_schema('id'), int_schema('value')),
        )));

        $window = window()->orderBy(ref('id'));
        $segments->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array(ExecutedSegments::of($segments, $context));

        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);

        static::assertEquals(1, $result[0]->column('row_num')->value(0));
        static::assertEquals(2, $result[0]->column('row_num')->value(1));
        static::assertEquals(3, $result[0]->column('row_num')->value(2));
    }

    public function test_sorts_partition_by_order_by(): void
    {
        $segments = new Segments(from_rows(array_to_rows(
            [
                ['dept' => 'IT', 'salary' => 6000],
                ['dept' => 'IT', 'salary' => 5000],
                ['dept' => 'IT', 'salary' => 7000],
            ],
            schema(str_schema('dept'), int_schema('salary')),
        )));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $segments->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array(ExecutedSegments::of($segments, $context));

        static::assertEquals(5000, $result[0]->column('salary')->value(0));
        static::assertEquals(6000, $result[0]->column('salary')->value(1));
        static::assertEquals(7000, $result[0]->column('salary')->value(2));

        static::assertEquals(1, $result[0]->column('row_num')->value(0));
        static::assertEquals(2, $result[0]->column('row_num')->value(1));
        static::assertEquals(3, $result[0]->column('row_num')->value(2));
    }
}
