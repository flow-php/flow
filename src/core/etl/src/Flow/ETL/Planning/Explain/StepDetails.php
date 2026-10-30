<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Explain;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\ResidentRowsStorage;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Storage\SpillingRowsStorage;
use Flow\ETL\Values\Schema\Definition;
use Flow\Telemetry\ObjectExtractor;

use function array_map;
use function implode;

final readonly class StepDetails
{
    private const string INDENT = '   ';

    public function __construct(
        private StatisticsLine $statistics = new StatisticsLine(),
        private NodeDetails $node = new NodeDetails(),
    ) {}

    /**
     * @return list<string>
     */
    public function lines(Extractor|JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer $step): array
    {
        $lines = [
            match (true) {
                $step instanceof Extractor => 'Extractor: ',
                $step instanceof JoinStep => 'Join: ',
                $step instanceof BranchPipeline => 'Branch: ',
                $step instanceof TransactionGroup => 'Transaction: ',
                $step instanceof Processor => 'Processor: ',
                $step instanceof Transformer => 'Transformer: ',
                default => 'Loader: ',
            } . ObjectExtractor::shortName($step instanceof JoinStep ? $step->join : $step),
        ];

        foreach ($this->settings($step) as $setting) {
            $lines[] = self::INDENT . $setting;
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function settings(Extractor|JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer $step): array
    {
        return match (true) {
            $step instanceof JoinStep && $step->join instanceof HashJoin => $this->hashJoin($step->join),
            $step instanceof JoinStep => $this->node->crossJoin($step->join->prefix),
            $step instanceof MergeSortProcessor => [
                'Sort by: ' . $this->node->sort($step->columns),
                'Memory: ' . $step->sort->memoryLimit->inMb() . ' MB',
                'Spill: ' . ObjectExtractor::shortName($step->sort->spill->storage()),
                'Merge: ' . $step->sort->mergeFanIn . ' ways',
                'Batch: ' . $step->sort->batchSize->size,
            ],
            $step instanceof MemorySortProcessor => ['Sort by: ' . $this->node->sort($step->columns)],
            $step instanceof TopNProcessor => ['Top: ' . $step->limit, 'Sort by: ' . $this->node->sort($step->columns)],
            $step instanceof GroupByAggregationProcessor => [
                'Group by: ' . $this->node->columns($step->groupBy->refs()->all()),
                'Aggregations: ' . $this->aggregations($step),
                'Memory: ' . $step->config->memoryLimit->inMb() . ' MB',
                'Storage: ' . $this->storageName($step->config->storage),
                'Batch: ' . $step->config->batchSize,
            ],
            $step instanceof PivotProcessor => $this->pivot($step),
            $step instanceof WindowProcessor => $this->window($step),
            $step instanceof RepartitionProcessor => [
                'By: ' . $this->node->columns($step->by->all()),
                'Memory: ' . $step->config->memoryLimit->inMb() . ' MB',
                'Storage: ' . $this->storageName($step->config->storage),
            ],
            $step instanceof BatchingProcessor => ['Batch: ' . $step->size->size],
            $step instanceof BatchingByProcessor => $this->batchingBy($step),
            $step instanceof CachingProcessor => $this->caching($step),
            $step instanceof ConstrainedProcessor => $step->constraints === []
                ? []
                : ['Constraints: ' . implode(', ', array_map(ObjectExtractor::shortName(...), $step->constraints))],
            $step instanceof CollectingProcessor => $step->declared === null ? [] : ['Schema: declared'],
            $step instanceof OffsetProcessor => ['Skip: ' . $step->offset],
            $step instanceof Extractor => $this->extractor($step),
            default => [],
        };
    }

    /**
     * @return list<string>
     */
    public function extractor(Extractor $extractor): array
    {
        $statistics = $this->statistics->of($extractor->statistics());

        return $statistics === null ? [] : ['Statistics: ' . $statistics];
    }

    /**
     * @return list<string>
     */
    public function hashJoin(HashJoin $join): array
    {
        return [
            ...$this->node->join($join->type, $join->expression),
            ...$this->storage($join->config->bucketing),
            'Buckets: ' . $join->config->bucketing->bucketsCount,
            'Batch: ' . $join->config->bucketing->batchSize,
        ];
    }

    /**
     * @return list<string>
     */
    public function pivot(PivotProcessor $pivot): array
    {
        $lines = ['Group by: ' . $this->node->columns($pivot->groupBy->refs()->all())];
        $pivoted = $pivot->groupBy->pivotedBy();

        if ($pivoted !== null) {
            $lines[] = 'Pivot: ' . $pivoted->column->name();
        }

        $lines[] = 'Batch: ' . $pivot->config->batchSize;

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function window(WindowProcessor $window): array
    {
        return [
            'Column: ' . ($window->column instanceof Definition ? $window->column->name() : $window->column),
            'Function: ' . ObjectExtractor::shortName($window->function),
            ...($window->bound === null ? [] : ['Frame: bound']),
        ];
    }

    /**
     * @return list<string>
     */
    public function batchingBy(BatchingByProcessor $batching): array
    {
        return (
            $batching->minSize === null
                ? ['Batch by: ' . $batching->column->name()]
                : ['Batch by: ' . $batching->column->name(), 'Min size: ' . $batching->minSize]
        );
    }

    /**
     * @return list<string>
     */
    public function caching(CachingProcessor $caching): array
    {
        $lines = ['Cache: ' . ObjectExtractor::shortName($caching->cache)];

        if ($caching->idGiven) {
            $lines[] = 'Id: ' . $caching->id;
        }

        return $lines;
    }

    public function aggregations(GroupByAggregationProcessor $groupBy): string
    {
        $names = [];

        foreach ($groupBy->groupBy->aggregations() as $aggregation) {
            $names[] = ObjectExtractor::shortName($aggregation);
        }

        return $names === [] ? 'none' : implode(', ', $names);
    }

    /**
     * @return list<string> the configured storage, and the memory limit it stands behind
     */
    public function storage(BucketingConfig $bucketing): array
    {
        return (
            $bucketing->storage instanceof ResidentRowsStorage
                ? ['Storage: ' . $this->storageName($bucketing->storage)]
                : [
                    'Storage: ' . $this->storageName($bucketing->storage),
                    'Memory: ' . $bucketing->memoryLimit->inMb() . ' MB',
                ]
        );
    }

    public function storageName(RowsStorage $storage): string
    {
        return ObjectExtractor::shortName($storage instanceof SpillingRowsStorage ? $storage->disk : $storage);
    }
}
