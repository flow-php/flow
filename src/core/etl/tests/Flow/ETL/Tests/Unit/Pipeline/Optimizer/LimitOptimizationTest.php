<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Optimizer;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\CSV\CSVExtractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\GroupBy;
use Flow\ETL\Pipeline\GroupByPipeline;
use Flow\ETL\Pipeline\Optimizer;
use Flow\ETL\Pipeline\Optimizer\LimitOptimization;
use Flow\ETL\Pipeline\PartitioningPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use PHPUnit\Framework\TestCase;

final class LimitOptimizationTest extends TestCase
{
    public function test_optimization_against_pipelines() : void
    {
        $this->assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new GroupByPipeline(new GroupBy(), new SynchronousPipeline()))
        );
        $this->assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new PartitioningPipeline(new SynchronousPipeline()))
        );
        // Pipeline without extractor
        $this->assertFalse(
            (new LimitOptimization())->isFor(new LimitTransformer(10), new SynchronousPipeline())
        );
    }

    public function test_optimization_for_a_pipeline_with_expanding_expression_transformations() : void
    {
        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new EntryExpressionEvalTransformer('expanded', ref('data')->expand()));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $this->assertFalse($pipeline->source()->isLimited());
        $this->assertCount(2, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_for_a_pipeline_with_expanding_transformations() : void
    {
        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new DropDuplicatesTransformer(ref('id')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $this->assertFalse($pipeline->source()->isLimited());
        $this->assertCount(2, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_for_a_pipeline_with_limited_extractor() : void
    {
        $pipeline = new SynchronousPipeline();
        $extractor = new CSVExtractor(Path::realpath('file.csv'));
        $extractor->changeLimit(10);
        $pipeline->setSource($extractor);
        $pipeline->add(new RenameEntryTransformer('id', 'new_id'));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $this->assertTrue($pipeline->source()->isLimited());
        $this->assertCount(2, $optimizedPipeline->pipes()->all());
        $this->assertInstanceOf(LimitTransformer::class, $optimizedPipeline->pipes()->all()[1]);
    }

    public function test_optimization_for_a_pipeline_without_expanding_transformations() : void
    {
        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(new CSVExtractor(Path::realpath('file.csv')));
        $pipeline->add(new KeepEntriesTransformer(ref('id'), ref('name')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $this->assertTrue($pipeline->source()->isLimited());
        $this->assertCount(1, $optimizedPipeline->pipes()->all());
    }

    public function test_optimization_of_limit_on_empty_pipeline() : void
    {
        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(new CSVExtractor(Path::realpath('file.csv')));

        $optimizedPipeline = (new Optimizer(new LimitOptimization()))->optimize(new LimitTransformer(10), $pipeline);

        $this->assertTrue($pipeline->source()->isLimited());
        $this->assertCount(0, $optimizedPipeline->pipes()->all());
    }
}
