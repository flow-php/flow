<?php

declare(strict_types=1);

namespace Flow\ETL\Planner;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Executor\PhysicalPlan;
use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\Join\Join as JoinType;
use Flow\ETL\Join\JoinSteps;
use Flow\ETL\Loader;
use Flow\ETL\Loader\SchemaValidationLoader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Processor\CountingProcessor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Repartition\RepartitionSteps;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CollectReferencesTransformer;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;
use Flow\ETL\Transformer\DropDuplicatesTransformer;
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\Transformer\DuplicateRowTransformer;
use Flow\ETL\Transformer\JoinEachRowsTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEachEntryTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionFilterTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;
use Flow\ETL\Transformer\Stateful;
use Flow\ETL\Transformer\UntilTransformer;

final readonly class NodeTranslator
{
    /**
     * Called once per plan, so a step Flow constructs here is rebuilt every run and a second run never sees the
     * state the first left behind. An instance the USER constructed - a Loader, or a Transformer handed to
     * transform()/rows() - is theirs: it is handed back as it is, never reset.
     *
     * @param list<PhysicalPlan> $frames the physical plan of a join's right side; [] for any other node
     *
     * @throws InvalidLogicException when no translation exists for the node
     *
     * @return list<Loader|Processor|Transformer> in execution order; [] for a node that adds no step of its own
     */
    public static function toSteps(Node $node, FlowContext $context, array $frames): array
    {
        return match (true) {
            $node instanceof Node\Read,
            $node instanceof Node\Result,
            $node instanceof Node\Outputs,
            $node instanceof Node\Transaction,
                => [],
            $node instanceof Node\Aggregate => GroupBySteps::of($node->groupBy, $context->config, $node->algorithm),
            $node instanceof Node\BatchBy => [new BatchingByProcessor($node->column, $node->minSize)],
            $node instanceof Node\Batch => [new BatchingProcessor($node->size)],
            $node instanceof Node\Cache => $node->batchSize
                ? [new BatchingProcessor($node->batchSize), new CachingProcessor($node->id, $node->cache)]
                : [new CachingProcessor($node->id, $node->cache)],
            $node instanceof Node\Collect => [new CollectingProcessor()],
            $node instanceof Node\CollectRefs => [new CollectReferencesTransformer($node->references)],
            $node instanceof Node\Constrain => [new ConstrainedProcessor($node->constraints)],
            $node instanceof Node\Count => [new CountingProcessor()],
            $node instanceof Node\CrossJoin => [new CrossJoinRowsTransformer(
                $frames[0],
                $context->config->executor(),
                $node->prefix,
            )],
            $node instanceof Node\Discard => [new VoidProcessor()],
            $node instanceof Node\Distinct => [new DropDuplicatesTransformer(...$node->entries)],
            $node instanceof Node\Drop => [new DropEntriesTransformer(...$node->entries)],
            $node instanceof Node\DuplicateRow => [new DuplicateRowTransformer($node->condition, ...$node->entries)],
            $node instanceof Node\Filter => [new ScalarFunctionFilterTransformer($node->function)],
            $node instanceof Node\JoinEach => match ($node->type) {
                JoinType::left => [JoinEachRowsTransformer::left($node->factory, $node->on)],
                JoinType::left_anti => [JoinEachRowsTransformer::leftAnti($node->factory, $node->on)],
                JoinType::right => [JoinEachRowsTransformer::right($node->factory, $node->on)],
                JoinType::inner => [JoinEachRowsTransformer::inner($node->factory, $node->on)],
            },
            $node instanceof Node\Join => JoinSteps::of(
                $frames[0],
                $node->on,
                $node->type,
                $context->config,
                $node->algorithm,
            ),
            $node instanceof Node\Limit => [new LimitTransformer($node->limit)],
            $node instanceof Node\Offset => [new OffsetProcessor($node->offset)],
            $node instanceof Node\RenameEach => [new RenameEachEntryTransformer(...$node->strategies)],
            $node instanceof Node\Rename => [new RenameEntryTransformer($node->from, $node->to)],
            $node instanceof Node\Repartition => RepartitionSteps::of($node->by, $context->config),
            $node instanceof Node\Select => [new SelectEntriesTransformer(...$node->entries)],
            $node instanceof Node\Sort => SortSteps::of($node->refs, $context->config, $node->algorithm),
            $node instanceof Node\TopN => [new TopNProcessor($node->refs, $node->limit)],
            $node instanceof Node\Transform => [
                $node->transformer instanceof Stateful ? $node->transformer->fresh() : $node->transformer,
            ],
            $node instanceof Node\Until => [new UntilTransformer($node->function)],
            $node instanceof Node\Validate => [new SchemaValidationLoader($node->schema, $node->validator)],
            $node instanceof Node\WindowColumn => $node->function->window()->partitions()->count()
                ? [
                    ...RepartitionSteps::of($node->function->window()->partitions(), $context->config),
                    new WindowProcessor($node->entry, $node->function),
                ]
                : [new CollectingProcessor(), new WindowProcessor($node->entry, $node->function)],
            $node instanceof Node\WithColumn => [new ScalarFunctionTransformer($node->entry, $node->function)],
            $node instanceof Node\Write => [$node->loader],
            default => throw InvalidLogicException::nodeNotTranslatable($node::class),
        };
    }
}
