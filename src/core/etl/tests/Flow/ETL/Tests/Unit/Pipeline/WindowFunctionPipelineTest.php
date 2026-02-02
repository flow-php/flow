<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{config, flow_context, from_rows, int_entry, ref, row, row_number, rows, str_entry, window};
use function Flow\ETL\DSL\sum;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor\WindowProcessor;
use PHPUnit\Framework\TestCase;

final class WindowFunctionPipelineTest extends TestCase
{
    public function test_handles_empty_input() : void
    {
        $pipeline = new Pipeline(from_rows(rows()));

        $window = (window())->orderBy(ref('id'));
        $pipeline->add(new WindowProcessor('row_num', (row_number())->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(0, $result);
    }

    public function test_handles_no_order_by() : void
    {
        $pipeline = new Pipeline(
            from_rows(
                rows(
                    row(str_entry('dept', 'IT'), int_entry('value', 100)),
                    row(str_entry('dept', 'IT'), int_entry('value', 150))
                )
            )
        );

        $window = (window())->partitionBy(ref('dept'));

        $pipeline->add(new WindowProcessor('total', (sum(ref('value')))->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(1, $result);
        self::assertCount(2, $result[0]);

        self::assertEquals(250, $result[0][0]->get('total')->value());
        self::assertEquals(250, $result[0][1]->get('total')->value());
    }

    public function test_handles_single_row_partition() : void
    {
        $pipeline = new Pipeline(
            from_rows(
                rows(
                    row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
                    row(str_entry('dept', 'HR'), int_entry('salary', 4000))
                )
            )
        );

        $window = (window())
            ->partitionBy(ref('dept'))
            ->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', (row_number())->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(2, $result);
        self::assertCount(1, $result[0]);
        self::assertCount(1, $result[1]);
    }

    public function test_processes_multiple_partitions_separately() : void
    {
        $pipeline = new Pipeline(
            from_rows(
                rows(
                    row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
                    row(str_entry('dept', 'IT'), int_entry('salary', 6000)),
                    row(str_entry('dept', 'HR'), int_entry('salary', 4000)),
                    row(str_entry('dept', 'HR'), int_entry('salary', 4500))
                )
            )
        );

        $window = (window())
            ->partitionBy(ref('dept'))
            ->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', (row_number())->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(2, $result);

        self::assertCount(2, $result[0]);
        self::assertEquals(1, $result[0][0]->get('row_num')->value());
        self::assertEquals(2, $result[0][1]->get('row_num')->value());

        self::assertCount(2, $result[1]);
        self::assertEquals(1, $result[1][0]->get('row_num')->value());
        self::assertEquals(2, $result[1][1]->get('row_num')->value());
    }

    public function test_processes_single_partition_without_partition_by() : void
    {
        $pipeline = new Pipeline(
            from_rows(
                rows(
                    row(int_entry('id', 1), int_entry('value', 100)),
                    row(int_entry('id', 2), int_entry('value', 150)),
                    row(int_entry('id', 3), int_entry('value', 200))
                )
            )
        );

        $window = (window())->orderBy(ref('id'));
        $pipeline->add(new WindowProcessor('row_num', (row_number())->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(1, $result);
        self::assertCount(3, $result[0]);

        self::assertEquals(1, $result[0][0]->get('row_num')->value());
        self::assertEquals(2, $result[0][1]->get('row_num')->value());
        self::assertEquals(3, $result[0][2]->get('row_num')->value());
    }

    public function test_sorts_partition_by_order_by() : void
    {
        $pipeline = new Pipeline(
            from_rows(
                rows(
                    row(str_entry('dept', 'IT'), int_entry('salary', 6000)),
                    row(str_entry('dept', 'IT'), int_entry('salary', 5000)),
                    row(str_entry('dept', 'IT'), int_entry('salary', 7000))
                )
            )
        );

        $window = (window())
            ->partitionBy(ref('dept'))
            ->orderBy(ref('salary'));

        $pipeline->add(new WindowProcessor('row_num', (row_number())->over($window)));

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertEquals(5000, $result[0][0]->get('salary')->value());
        self::assertEquals(6000, $result[0][1]->get('salary')->value());
        self::assertEquals(7000, $result[0][2]->get('salary')->value());

        self::assertEquals(1, $result[0][0]->get('row_num')->value());
        self::assertEquals(2, $result[0][1]->get('row_num')->value());
        self::assertEquals(3, $result[0][2]->get('row_num')->value());
    }
}
