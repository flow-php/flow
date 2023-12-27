<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline\Optimizer;

use function Flow\ETL\DSL\any;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Filesystem\LocalFilesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Partition\FiltersCollection;
use Flow\ETL\Partition\ScalarFunctionFilter;
use Flow\ETL\Pipeline\Optimizer\PartitionPruningOptimization;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use Flow\ETL\Tests\Integration\Pipeline\Optimizer\Doubles\FilePartitionExtractorSpy;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use PHPUnit\Framework\TestCase;

final class PartitionPruningOptimizationTest extends TestCase
{
    public function test_applying_multiple_partition_filters_grouped_by_any() : void
    {
        $optimizer = new PartitionPruningOptimization(
            new LocalFilesystem(),
            $entryFactory = new NativeEntryFactory()
        );

        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(
            $extractor = new FilePartitionExtractorSpy(
                new Path(__DIR__ . '/Fixtures/partition_pruning_optimization/group=*/*.txt')
            )
        );

        $optimizer->optimize(
            new ScalarFunctionFilterTransformer(
                $partitionFilter = any(
                    ref('group')->equals(lit('a')),
                    ref('group')->equals(lit('b'))
                ),
            ),
            $pipeline
        );

        $this->assertCount(0, $pipeline->pipes()->all());
        $this->assertEquals(new ScalarFunctionFilter($partitionFilter, $entryFactory), $extractor->partitionFilter());
    }

    public function test_applying_multiple_partition_filters_set_one_by_one() : void
    {
        $optimizer = new PartitionPruningOptimization(
            new LocalFilesystem(),
            $entryFactory = new NativeEntryFactory()
        );

        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(
            $extractor = new FilePartitionExtractorSpy(
                new Path(__DIR__ . '/Fixtures/partition_pruning_optimization/group=*/*.txt')
            )
        );

        $optimizer->optimize(
            new ScalarFunctionFilterTransformer($partitionFilter1 = ref('group')->equals(lit('a'))),
            $pipeline
        );
        $optimizer->optimize(
            new ScalarFunctionFilterTransformer($partitionFilter2 = ref('group')->equals(lit('b'))),
            $pipeline
        );

        $this->assertCount(0, $pipeline->pipes()->all());
        $this->assertEquals(
            new FiltersCollection([
                new ScalarFunctionFilter($partitionFilter1, $entryFactory),
                new ScalarFunctionFilter($partitionFilter2, $entryFactory),
            ]),
            $extractor->partitionFilter()
        );
    }

    public function test_applying_single_partition_filter() : void
    {
        $optimizer = new PartitionPruningOptimization(
            new LocalFilesystem(),
            $entryFactory = new NativeEntryFactory()
        );

        $pipeline = new SynchronousPipeline();
        $pipeline->setSource(
            $extractor = new FilePartitionExtractorSpy(
                new Path(__DIR__ . '/Fixtures/partition_pruning_optimization/group=*/*.txt')
            )
        );

        $optimizer->optimize(
            new ScalarFunctionFilterTransformer(
                $partitionFilter = ref('group')->equals(lit('a')),
            ),
            $pipeline
        );

        $this->assertCount(0, $pipeline->pipes()->all());
        $this->assertEquals(new ScalarFunctionFilter($partitionFilter, $entryFactory), $extractor->partitionFilter());
    }
}
