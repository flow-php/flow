<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Physical;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\RandomValueGenerator;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\SchemaValidationLoader;
use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\Constraint;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Join\HashJoin;
use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\CountingProcessor;
use Flow\ETL\Operators\Processor\DistinctProcessor;
use Flow\ETL\Operators\Processor\ExpandingProcessor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\RepartitionProcessor;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Operators\Transformer\CollectReferencesTransformer;
use Flow\ETL\Operators\Transformer\DropColumnsTransformer;
use Flow\ETL\Operators\Transformer\DuplicateRowTransformer;
use Flow\ETL\Operators\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Operators\Transformer\LimitTransformer;
use Flow\ETL\Operators\Transformer\PruneColumnsTransformer;
use Flow\ETL\Operators\Transformer\RenameColumnTransformer;
use Flow\ETL\Operators\Transformer\RenameEachColumnTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Operators\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Operators\Transformer\SelectColumnsTransformer;
use Flow\ETL\Operators\Transformer\Stateful;
use Flow\ETL\Operators\Transformer\UntilTransformer;
use Flow\ETL\Planning\Logical\Node;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\RowsStorage;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;
use Flow\Filesystem\Path;

use function array_filter;
use function array_map;
use function array_values;

final readonly class NodeTranslator
{
    public function __construct(
        private Backend $backend,
        private RandomValueGenerator $randomValueGenerator,
        private string $id,
        private RowsStorage $cache,
        private Path $cacheDir,
        private MemorySortConfig|ExternalSortConfig $sort,
        private BucketingConfig $grouping,
        private HashJoinConfig $join,
        private BucketingConfig $repartition,
    ) {}

    /**
     * Called once per plan, so a step Flow constructs here is rebuilt every run and a second run never sees the
     * state the first left behind. An instance the USER constructed - a Loader, or a Transformer handed to
     * transform()/rows() - is theirs: it is handed back as it is, never reset.
     *
     * @throws InvalidLogicException when no translation exists for the node
     *
     * @return list<CrossJoin|HashJoin|Loader|Processor|Transformer> in execution order, a join as its operator alone;
     *                                                              [] for a node that adds no step of its own
     */
    public function steps(Node $node): array
    {
        if ($node instanceof Node\Write && $node->loader instanceof FileLoader) {
            Part::refuseReserved($node->loader->destination(), $node->loader->partitioning());
        }

        return match (true) {
            $node instanceof Node\Read,
            $node instanceof Node\Result,
            $node instanceof Node\Outputs,
            $node instanceof Node\Transaction,
                => [],
            $node instanceof Node\Aggregate && $node->groupBy->isPivot() => [new PivotProcessor(
                $node->groupBy,
                $node->algorithm?->build($this->cacheDir, $this->backend) ?? $this->grouping,
            )],
            $node instanceof Node\Aggregate => [
                ...array_map(
                    static fn(array $columns): PruneColumnsTransformer => new PruneColumnsTransformer(...$columns),
                    array_filter([$node->groupBy->readColumns()], is_array(...)),
                ),
                new GroupByAggregationProcessor(
                    $node->groupBy,
                    $node->algorithm?->build($this->cacheDir, $this->backend) ?? $this->grouping,
                    $this->randomValueGenerator,
                ),
            ],
            $node instanceof Node\BatchBy => [new BatchingByProcessor($node->column, $node->minSize)],
            $node instanceof Node\Batch => [new BatchingProcessor($node->size)],
            $node instanceof Node\Cache => $node->batchSize
                ? [
                    new BatchingProcessor(new BatchSize($node->batchSize)),
                    new CachingProcessor($node->id ?: $this->id, $node->cache ?? $this->cache, (bool) $node->id),
                ]
                : [new CachingProcessor($node->id ?: $this->id, $node->cache ?? $this->cache, (bool) $node->id)],
            $node instanceof Node\Collect => [new CollectingProcessor()],
            $node instanceof Node\CollectRefs => [new CollectReferencesTransformer($node->references)],
            $node instanceof Node\Constrain => [new ConstrainedProcessor(array_map(
                static fn(Constraint $constraint): Constraint => $constraint instanceof Stateful
                    ? $constraint->fresh()
                    : $constraint,
                $node->constraints,
            ))],
            $node instanceof Node\Count => [new CountingProcessor()],
            $node instanceof Node\CrossJoin => [new CrossJoin($node->prefix)],
            $node instanceof Node\Discard => [new VoidProcessor()],
            $node instanceof Node\Distinct => [new DistinctProcessor(array_values($node->columns))],
            $node instanceof Node\Drop => [new DropColumnsTransformer(...$node->columns)],
            $node instanceof Node\ExpandColumn => [
                new ExpandingProcessor($node->column, $node->function, $node->carries),
            ],
            $node instanceof Node\DuplicateRow => [new DuplicateRowTransformer($node->condition, $node->columns)],
            $node instanceof Node\Filter => [new ScalarFunctionFilterTransformer($node->function)],
            $node instanceof Node\JoinEach => match ($node->type) {
                JoinType::left => [JoinEachRowsTransformer::left($node->factory, $node->on)],
                JoinType::left_anti => [JoinEachRowsTransformer::leftAnti($node->factory, $node->on)],
                JoinType::right => [JoinEachRowsTransformer::right($node->factory, $node->on)],
                JoinType::inner => [JoinEachRowsTransformer::inner($node->factory, $node->on)],
            },
            $node instanceof Node\Join => [new HashJoin(
                $node->on,
                $node->type,
                $node->algorithm?->build($this->cacheDir, $this->backend) ?? $this->join,
                $this->randomValueGenerator,
            )],
            $node instanceof Node\Limit => [new LimitTransformer($node->limit)],
            $node instanceof Node\Offset => [new OffsetProcessor($node->offset)],
            $node instanceof Node\RenameEach => [new RenameEachColumnTransformer(...$node->strategies)],
            $node instanceof Node\Rename => [new RenameColumnTransformer($node->from, $node->to)],
            $node instanceof Node\Repartition => [new RepartitionProcessor(
                $node->by,
                $this->repartition,
                $this->randomValueGenerator,
            )],
            $node instanceof Node\Select => [new SelectColumnsTransformer(...$node->columns)],
            $node instanceof Node\Sort => array_map(
                fn(MemorySortConfig|ExternalSortConfig $sort): Processor => $sort instanceof ExternalSortConfig
                    ? new MergeSortProcessor($sort, $node->columns, $this->randomValueGenerator)
                    : new MemorySortProcessor($node->columns),
                [$node->algorithm?->build($this->cacheDir, $this->backend) ?? $this->sort],
            ),
            $node instanceof Node\TopN => [new TopNProcessor(
                $node->columns,
                $node->limit,
                $node->algorithm?->build($this->cacheDir, $this->backend) ?? $this->sort,
                $this->randomValueGenerator,
            )],
            $node instanceof Node\Transform => [
                $node->transformer instanceof Stateful ? $node->transformer->fresh() : $node->transformer,
            ],
            $node instanceof Node\Until => [new UntilTransformer($node->function)],
            $node instanceof Node\Validate => [new SchemaValidationLoader($node->schema, $node->validator)],
            $node instanceof Node\WindowColumn => $node->function->window()->partitions()->count()
                ? [
                    new RepartitionProcessor(
                        $node->function->window()->partitions(),
                        $this->repartition,
                        $this->randomValueGenerator,
                    ),
                    new WindowProcessor($node->column, $node->function),
                ]
                : [new CollectingProcessor(), new WindowProcessor($node->column, $node->function)],
            $node instanceof Node\WithColumn => [new ScalarFunctionTransformer($node->column, $node->function)],
            $node instanceof Node\Write => [$node->loader],
            default => throw InvalidLogicException::nodeNotTranslatable($node::class),
        };
    }
}
