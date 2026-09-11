<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\GroupBy;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Row\References;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function count;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\from_rows;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path_real;

final class LimitOptimizationTest extends FlowTestCase
{
    public function test_limit_is_not_pushed_past_a_filter(): void
    {
        $filtered = from_csv(path_real('file.csv'));
        $filteredPipeline = new Pipeline($filtered);
        $filteredPipeline->add(new ScalarFunctionFilterTransformer(ref('id')->equals(lit(1))));

        (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $filteredPipeline);

        $selectedThenFiltered = from_csv(path_real('file.csv'));
        $selectedThenFilteredPipeline = new Pipeline($selectedThenFiltered);
        $selectedThenFilteredPipeline->add(new SelectEntriesTransformer(ref('id')));
        $selectedThenFilteredPipeline->add(new ScalarFunctionFilterTransformer(ref('id')->equals(lit(1))));

        (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $selectedThenFilteredPipeline);

        static::assertNull($filtered->pushedLimit());
        static::assertNull($selectedThenFiltered->pushedLimit());
    }

    public function test_limit_transformer_stays_in_the_pipeline_after_push_down(): void
    {
        $accepted = new Pipeline(from_csv(path_real('file.csv')));
        $accepted->add(new SelectEntriesTransformer(ref('id')));

        $refused = new Pipeline(from_csv(path_real('file.csv')));
        $refused->add(new DropDuplicatesTransformer(ref('id')));

        $expanding = new Pipeline(from_csv(path_real('file.csv')));
        $expanding->add(new ScalarFunctionTransformer('expanded', ref('data')->expand()));

        $notPushing = new Pipeline(from_rows(rows(schema())));

        foreach ([$accepted, $refused, $expanding, $notPushing] as $pipeline) {
            $steps = (new Optimizer(new LimitOptimization()))
                ->optimize(new LimitTransformer(10), $pipeline)
                ->segments()
                ->steps();

            static::assertInstanceOf(LimitTransformer::class, $steps[count($steps) - 1]);
        }
    }

    public function test_push_down_leaves_the_callers_extractor_untouched(): void
    {
        $extractor = from_csv(path_real('file.csv'));
        $pipeline = new Pipeline($extractor);

        (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $pushed = $pipeline->extractor();

        static::assertInstanceOf(CSVExtractor::class, $pushed);
        static::assertNotSame($extractor, $pushed);
        static::assertSame(10, $pushed->pushedLimit());
        static::assertNull($extractor->pushedLimit());
    }

    public function test_optimization_against_pipelines_with_expanding_processors(): void
    {
        $groupedExtractor = from_csv(path_real('file.csv'));
        $pipelineWithGroupBy = new Pipeline($groupedExtractor);
        $pipelineWithGroupBy->add(new GroupByAggregationProcessor(new GroupBy(), new Buckets(new MemoryBuckets())));

        (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipelineWithGroupBy);

        static::assertNull($groupedExtractor->pushedLimit());

        $partitionedExtractor = from_csv(path_real('file.csv'));
        $pipelineWithPartitioning = new Pipeline($partitionedExtractor);
        $pipelineWithPartitioning->add(
            new RepartitionProcessor(References::init(ref('group')), new Buckets(new MemoryBuckets())),
        );

        (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipelineWithPartitioning);

        static::assertNull($partitionedExtractor->pushedLimit());
    }

    public function test_optimization_for_a_pipeline_with_expanding_expression_transformations(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));
        $pipeline->add(new ScalarFunctionTransformer('expanded', ref('data')->expand()));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertNull($extractor->pushedLimit());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_for_a_pipeline_with_expanding_transformations(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));
        $pipeline->add(new DropDuplicatesTransformer(ref('id')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertNull($extractor->pushedLimit());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_for_a_pipeline_with_limited_extractor(): void
    {
        $extractor = from_csv(path_real('file.csv'));
        $extractor->pushLimit(10);
        $pipeline = new Pipeline($extractor);
        $pipeline->add(new RenameEntryTransformer('id', 'new_id'));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertNotNull($extractor->pushedLimit());
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
        static::assertNotNull($extractor->pushedLimit());
        static::assertCount(2, $optimizedPipeline->segments()->steps());
    }

    public function test_optimization_of_limit_on_empty_pipeline(): void
    {
        $pipeline = new Pipeline(from_csv(path_real('file.csv')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $extractor = $pipeline->extractor();
        static::assertInstanceOf(CSVExtractor::class, $extractor);
        static::assertNotNull($extractor->pushedLimit());
        static::assertCount(1, $optimizedPipeline->segments()->steps());
    }
}
