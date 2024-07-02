<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\GroupBy;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\Pipeline\{GroupByPipeline, Optimizer, PartitioningPipeline, SynchronousPipeline};
use Flow\ETL\Transformer\{DropDuplicatesTransformer, LimitTransformer, RenameEntryTransformer, ScalarFunctionTransformer, SelectEntriesTransformer};
use Flow\Filesystem\Path;
use PHPUnit\Framework\TestCase;

final class LimitOptimizationTest extends TestCase
{
    public function test_optimization_against_pipelines() : void
    {
        self::assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new GroupByPipeline(new GroupBy(), new SynchronousPipeline()))
        );
        self::assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new PartitioningPipeline(new SynchronousPipeline(), [ref('group')]))
        );
        // Pipeline without extractor
        self::assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new SynchronousPipeline())
        );
    }

    public function test_optimization_for_a_pipeline_with_expanding_expression_transformations() : void
    {
        $pipeline = new SynchronousPipeline(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new ScalarFunctionTransformer('expanded', ref('data')->expand()));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        self::assertFalse($pipeline->source()->isLimited());
        self::assertCount(2, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_for_a_pipeline_with_expanding_transformations() : void
    {
        $pipeline = new SynchronousPipeline(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new DropDuplicatesTransformer(ref('id')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        self::assertFalse($pipeline->source()->isLimited());
        self::assertCount(2, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_for_a_pipeline_with_limited_extractor() : void
    {
        $extractor = new CSVExtractor(Path::realpath('file.csv'));
        $extractor->changeLimit(10);
        $pipeline = new SynchronousPipeline($extractor);
        $pipeline->add(new RenameEntryTransformer('id', 'new_id'));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        self::assertTrue($pipeline->source()->isLimited());
        self::assertCount(2, $optimizedPipeline->pipes()->all());
        self::assertInstanceOf(LimitTransformer::class, $optimizedPipeline->pipes()->all()[1]);
    }

    public function test_optimization_for_a_pipeline_without_expanding_transformations() : void
    {
        $pipeline = new SynchronousPipeline(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new SelectEntriesTransformer(ref('id'), ref('name')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        self::assertTrue($pipeline->source()->isLimited());
        self::assertCount(1, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_of_limit_on_empty_pipeline() : void
    {
        $pipeline = new SynchronousPipeline(new CSVExtractor(Path::realpath('file.csv')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        self::assertTrue($pipeline->source()->isLimited());
        self::assertCount(0, $optimizedPipeline->pipes()->all());
    }
}
