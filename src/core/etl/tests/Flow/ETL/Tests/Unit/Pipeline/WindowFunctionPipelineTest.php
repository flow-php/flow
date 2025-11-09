<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline;

use function Flow\ETL\DSL\{config, flow_context, from_rows, int_entry, ref, row, row_number, rows, str_entry, window};
use function Flow\ETL\DSL\sum;
use Flow\ETL\{Extractor, Loader};
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\OverridingPipeline;
use Flow\ETL\Pipeline\{SynchronousPipeline, WindowFunctionPipeline};
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use PHPUnit\Framework\TestCase;

final class WindowFunctionPipelineTest extends TestCase
{
    public function test_add_adds_loader_to_underlying_pipeline() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $loader = $this->createMock(Loader::class);
        $result = $pipeline->add($loader);

        self::assertSame($pipeline, $result);
    }

    public function test_add_adds_transformer_to_underlying_pipeline() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $transformer = new ScalarFunctionTransformer('test', ref('id'));
        $result = $pipeline->add($transformer);

        self::assertSame($pipeline, $result);
        self::assertTrue($pipeline->has(ScalarFunctionTransformer::class));
    }

    public function test_handles_empty_input() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $window = (window())->orderBy(ref('id'));
        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over($window)
        );

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(0, $result);
    }

    public function test_handles_no_order_by() : void
    {
        $basePipeline = new SynchronousPipeline(
            from_rows(
                rows(
                    row(str_entry('dept', 'IT'), int_entry('value', 100)),
                    row(str_entry('dept', 'IT'), int_entry('value', 150))
                )
            )
        );

        $window = (window())->partitionBy(ref('dept'));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'total',
            (sum(ref('value')))->over($window)
        );

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(1, $result);
        self::assertCount(2, $result[0]);

        self::assertEquals(250, $result[0][0]->get('total')->value());
        self::assertEquals(250, $result[0][1]->get('total')->value());
    }

    public function test_handles_single_row_partition() : void
    {
        $basePipeline = new SynchronousPipeline(
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

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over($window)
        );

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertCount(2, $result);
        self::assertCount(1, $result[0]);
        self::assertCount(1, $result[1]);
    }

    public function test_has_returns_false_when_transformer_not_present() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        self::assertFalse($pipeline->has(ScalarFunctionTransformer::class));
    }

    public function test_has_returns_true_when_transformer_present() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $transformer = new ScalarFunctionTransformer('test', ref('id'));
        $pipeline->add($transformer);

        self::assertTrue($pipeline->has(ScalarFunctionTransformer::class));
    }

    public function test_implements_overriding_pipeline_interface() : void
    {
        $pipeline = new WindowFunctionPipeline(
            new SynchronousPipeline(from_rows(rows())),
            'row_num',
            (row_number())->over(window())
        );

        self::assertInstanceOf(OverridingPipeline::class, $pipeline);
    }

    public function test_implements_pipeline_interface() : void
    {
        $pipeline = new WindowFunctionPipeline(
            new SynchronousPipeline(from_rows(rows())),
            'row_num',
            (row_number())->over(window())
        );

        self::assertInstanceOf(Pipeline::class, $pipeline);
    }

    public function test_pipelines_returns_array_with_underlying_pipeline() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $pipelines = $pipeline->pipelines();

        self::assertCount(1, $pipelines);
        self::assertSame($basePipeline, $pipelines[0]);
    }

    public function test_pipes_returns_pipes_from_underlying_pipeline() : void
    {
        $basePipeline = new SynchronousPipeline(from_rows(rows()));

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $transformer = new ScalarFunctionTransformer('test', ref('id'));
        $pipeline->add($transformer);

        $pipes = $pipeline->pipes();

        self::assertCount(1, $pipes->all());
        self::assertInstanceOf(ScalarFunctionTransformer::class, $pipes->all()[0]);
    }

    public function test_processes_multiple_partitions_separately() : void
    {
        $basePipeline = new SynchronousPipeline(
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

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over($window)
        );

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
        $basePipeline = new SynchronousPipeline(
            from_rows(
                rows(
                    row(int_entry('id', 1), int_entry('value', 100)),
                    row(int_entry('id', 2), int_entry('value', 150)),
                    row(int_entry('id', 3), int_entry('value', 200))
                )
            )
        );

        $window = (window())->orderBy(ref('id'));
        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over($window)
        );

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
        $basePipeline = new SynchronousPipeline(
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

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over($window)
        );

        $context = flow_context(config());
        $result = \iterator_to_array($pipeline->process($context));

        self::assertEquals(5000, $result[0][0]->get('salary')->value());
        self::assertEquals(6000, $result[0][1]->get('salary')->value());
        self::assertEquals(7000, $result[0][2]->get('salary')->value());

        self::assertEquals(1, $result[0][0]->get('row_num')->value());
        self::assertEquals(2, $result[0][1]->get('row_num')->value());
        self::assertEquals(3, $result[0][2]->get('row_num')->value());
    }

    public function test_source_returns_extractor_from_underlying_pipeline() : void
    {
        $extractor = from_rows(rows());
        $basePipeline = new SynchronousPipeline($extractor);

        $pipeline = new WindowFunctionPipeline(
            $basePipeline,
            'row_num',
            (row_number())->over(window())
        );

        $source = $pipeline->source();

        self::assertInstanceOf(Extractor::class, $source);
        self::assertSame($extractor, $source);
    }
}
