<?php

declare(strict_types=1);

namespace Flow\ETL\Plan\Explain;

use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\HashJoinProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\SortOrder;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

use function array_map;
use function implode;

final readonly class StepDetails
{
    private const string INDENT = '   ';

    public function __construct(
        private Details $details = new Details(),
        private Condition $condition = new Condition(),
        private StatisticsLine $statistics = new StatisticsLine(),
    ) {}

    /**
     * @return list<string>
     */
    public function lines(Extractor|Loader|Processor|Transformer $step): array
    {
        $label = match (true) {
            $step instanceof Extractor => 'Extractor: ',
            $step instanceof Processor => 'Processor: ',
            $step instanceof Transformer => 'Transformer: ',
            default => 'Loader: ',
        };

        $lines = [$label . $this->details->name($step)];

        foreach ($this->settings($step) as $setting) {
            $lines[] = self::INDENT . $setting;
        }

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function settings(Extractor|Loader|Processor|Transformer $step): array
    {
        return match (true) {
            $step instanceof HashJoinProcessor => $this->hashJoin($step),
            $step instanceof CrossJoinRowsTransformer => $step->prefix === ''
                ? ['Join: cross']
                : ['Join: cross', 'Prefix: ' . $step->prefix],
            $step instanceof MergeSortProcessor => [
                'Sort: ' . $this->sort($step->refs),
                'Spill: ' . $this->details->name($step->spill->storage()),
                'Merge: ' . $step->mergeFanIn . ' ways',
                'Batch: ' . $step->batchSize,
            ],
            $step instanceof MemorySortProcessor => ['Sort: ' . $this->sort($step->refs)],
            $step instanceof TopNProcessor => ['Top: ' . $step->limit, 'Sort: ' . $this->sort($step->refs)],
            $step instanceof GroupByAggregationProcessor => [
                'Group by: ' . $this->columns($step->groupBy->refs()),
                'Aggregations: ' . $this->aggregations($step),
                'Storage: ' . $this->details->name($step->buckets->storage()),
                'Batch: ' . $step->batchSize,
            ],
            $step instanceof PivotProcessor => $this->pivot($step),
            $step instanceof WindowProcessor => $this->window($step),
            $step instanceof RepartitionProcessor => [
                'By: ' . $this->columns($step->by),
                'Hasher: ' . $this->details->name($step->hasher),
                'Storage: ' . $this->details->name($step->buckets->storage()),
            ],
            $step instanceof BucketingProcessor => [
                'Strategy: ' . $this->details->name($step->strategy),
                'Storage: ' . $this->details->name($step->buckets->storage()),
            ],
            $step instanceof BatchingProcessor => ['Batch: ' . $step->size],
            $step instanceof BatchingByProcessor => $this->batchingBy($step),
            $step instanceof CachingProcessor => $this->caching($step),
            $step instanceof ConstrainedProcessor => $step->constraints === []
                ? []
                : ['Constraints: ' . implode(', ', array_map($this->details->name(...), $step->constraints))],
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
    public function hashJoin(HashJoinProcessor $join): array
    {
        $lines = ['Join: ' . $join->type->value];

        foreach ($this->condition->lines($join->expression->comparison()) as $condition) {
            $lines[] = 'On: ' . $condition;
        }

        if ($join->expression->prefix() !== '') {
            $lines[] = 'Prefix: ' . $join->expression->prefix();
        }

        return [
            ...$lines,
            'Storage: ' . $this->details->name($join->rightBuckets->storage()),
            'Buckets: ' . $join->bucketsCount,
            'Batch: ' . $join->batchSize,
        ];
    }

    /**
     * @return list<string>
     */
    public function pivot(PivotProcessor $pivot): array
    {
        $lines = ['Group by: ' . $this->columns($pivot->groupBy->refs())];
        $pivoted = $pivot->groupBy->pivotedBy();

        if ($pivoted !== null) {
            $lines[] = 'Pivot: ' . $pivoted->column->name();
        }

        $lines[] = 'Batch: ' . $pivot->batchSize;

        return $lines;
    }

    /**
     * @return list<string>
     */
    public function window(WindowProcessor $window): array
    {
        return [
            'Column: ' . ($window->entry instanceof Definition ? $window->entry->entry()->name() : $window->entry),
            'Function: ' . $this->details->name($window->function),
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
        $lines = $caching->cache === null ? [] : ['Cache: ' . $this->details->name($caching->cache)];

        if ($caching->id !== null) {
            $lines[] = 'Id: ' . $caching->id;
        }

        return $lines;
    }

    public function aggregations(GroupByAggregationProcessor $groupBy): string
    {
        $names = [];

        foreach ($groupBy->groupBy->aggregations() as $aggregation) {
            $names[] = $this->details->name($aggregation);
        }

        return $names === [] ? 'none' : implode(', ', $names);
    }

    public function columns(References $refs): string
    {
        return implode(', ', array_map(static fn(Reference $ref): string => $ref->name(), $refs->all()));
    }

    public function sort(References $refs): string
    {
        return implode(', ', array_map(
            static fn(Reference $ref): string => (
                $ref->name() . ' ' . ($ref->sort() === SortOrder::ASC ? 'asc' : 'desc')
            ),
            $refs->all(),
        ));
    }
}
