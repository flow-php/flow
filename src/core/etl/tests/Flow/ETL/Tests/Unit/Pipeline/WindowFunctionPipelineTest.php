<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use Flow\ETL\Pipeline;
use Flow\ETL\Processor\WindowProcessor;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function iterator_to_array;

final class WindowFunctionPipelineTest extends TestCase
{
    public function test_handles_empty_input(): void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        $window = window()->orderBy(ref('id'));
        $pipeline->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertCount(0, $result);
    }

    public function test_handles_no_order_by(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(str_entry('dept', 'IT'), int_entry('value', 100)),
            row(str_entry('dept', 'IT'), int_entry('value', 150)),
        )));

        $window = window()->partitionBy(ref('dept'));

        $pipeline->add(new WindowProcessor('total', sum(ref('value'))->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertCount(1, $result);
        static::assertCount(2, $result[0]);

        static::assertEquals(250, $result[0][0]->get('total')->value());
        static::assertEquals(250, $result[0][1]->get('total')->value());
    }

    public function test_handles_single_row_partition(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
            row(str_entry('dept', 'HR'), int_entry('salary', 4000)),
        )));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertCount(2, $result);
        static::assertCount(1, $result[0]);
        static::assertCount(1, $result[1]);
    }

    public function test_processes_multiple_partitions_separately(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
            row(str_entry('dept', 'IT'), int_entry('salary', 6000)),
            row(str_entry('dept', 'HR'), int_entry('salary', 4000)),
            row(str_entry('dept', 'HR'), int_entry('salary', 4500)),
        )));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertCount(2, $result);

        static::assertCount(2, $result[0]);
        static::assertEquals(1, $result[0][0]->get('row_num')->value());
        static::assertEquals(2, $result[0][1]->get('row_num')->value());

        static::assertCount(2, $result[1]);
        static::assertEquals(1, $result[1][0]->get('row_num')->value());
        static::assertEquals(2, $result[1][1]->get('row_num')->value());
    }

    public function test_processes_single_partition_without_partition_by(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(int_entry('id', 1), int_entry('value', 100)),
            row(int_entry('id', 2), int_entry('value', 150)),
            row(int_entry('id', 3), int_entry('value', 200)),
        )));

        $window = window()->orderBy(ref('id'));
        $pipeline->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertCount(1, $result);
        static::assertCount(3, $result[0]);

        static::assertEquals(1, $result[0][0]->get('row_num')->value());
        static::assertEquals(2, $result[0][1]->get('row_num')->value());
        static::assertEquals(3, $result[0][2]->get('row_num')->value());
    }

    public function test_sorts_partition_by_order_by(): void
    {
        $pipeline = new Pipeline(from_rows(rows(
            row(str_entry('dept', 'IT'), int_entry('salary', 6000)),
            row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
            row(str_entry('dept', 'IT'), int_entry('salary', 7000)),
        )));

        $window = window()->partitionBy(ref('dept'))->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', row_number()->over($window)));

        $context = flow_context(config());
        $result = iterator_to_array($pipeline->process($context));

        static::assertEquals(5000, $result[0][0]->get('salary')->value());
        static::assertEquals(6000, $result[0][1]->get('salary')->value());
        static::assertEquals(7000, $result[0][2]->get('salary')->value());

        static::assertEquals(1, $result[0][0]->get('row_num')->value());
        static::assertEquals(2, $result[0][1]->get('row_num')->value());
        static::assertEquals(3, $result[0][2]->get('row_num')->value());
    }
}
