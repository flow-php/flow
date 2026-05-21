<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\GroupBy;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\Processor\GroupByProcessor;
use Flow\ETL\Processor\PartitioningProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\Filesystem\DSL\path_real;

final class LimitOptimizationTest extends FlowTestCase
{
    public function test_optimization_against_pipelines_with_expanding_processors(): void
    {
        // Pipeline with GroupByProcessor - should not optimize
        $pipelineWithGroupBy = new Pipeline(from_csv(path_real('file.csv')));
        $pipelineWithGroupBy->add(new GroupByProcessor(new GroupBy()));

        static::assertFalse((new LimitOptimization())->isFor(new LimitTransformer(10), $pipelineWithGroupBy));

        // Pipeline with PartitioningProcessor - should not optimize
        $pipelineWithPartitioning = new Pipeline(from_csv(path_real('file.csv')));
        $pipelineWithPartitioning->add(new PartitioningProcessor([ref('group')]));

        static::assertFalse((new LimitOptimization())->isFor(new LimitTransformer(10), $pipelineWithPartitioning));

        // Pipeline with empty rows extractor - should not optimize
        $pipelineWithEmptyExtractor = new Pipeline(from_rows(rows()));

        static::assertFalse((new LimitOptimization())->isFor(new LimitTransformer(10), $pipelineWithEmptyExtractor));
    }

    public function test_optimization_for_a_pipeline_with_expanding_expression_transformations(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));
        $pipeline->add(new ScalarFunctionTransformer('expanded', ref('data')->expand()));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertFalse($extractor->isLimited());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_for_a_pipeline_with_expanding_transformations(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));
        $pipeline->add(new DropDuplicatesTransformer(ref('id')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertFalse($extractor->isLimited());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_for_a_pipeline_with_limited_extractor(): void
    {
        $extractor = from_csv(path_real('file.csv'));
        $extractor->changeLimit(10);
        $pipeline = new Pipeline($extractor);
        $pipeline->add(new RenameEntryTransformer('id', 'new_id'));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertTrue($extractor->isLimited());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
        static::assertInstanceOf(LimitTransformer::class, $optimizedPipeline->segments()->steps()[1]);
    }

    public function test_optimization_for_a_pipeline_without_expanding_transformations(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));
        $pipeline->add(new SelectEntriesTransformer(ref('id'), ref('name')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertTrue($extractor->isLimited());
        static::assertCount(1, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_of_limit_on_empty_pipeline(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertTrue($extractor->isLimited());
        static::assertCount(0, $optimizedPipeline->segments()->steps());
    }
}
