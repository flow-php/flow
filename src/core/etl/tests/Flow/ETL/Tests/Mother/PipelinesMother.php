<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Mother;

use Flow\ETL\Foundation\Exception\DataDependentSchemaException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\Node\Read;
use Flow\ETL\Planning\Physical\BranchPipeline;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Planning\Physical\PhysicalPlan;
use Flow\ETL\Planning\Physical\Pipeline;
use Flow\ETL\Planning\Physical\Planner;
use Flow\ETL\Planning\Physical\TransactionGroup;
use Flow\ETL\Tests\Double\RecordingSplittableExtractor;

use function count;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

final class PipelinesMother
{
    /**
     * $steps over $source cut at every breaker, as the planner cuts them: each pipeline reads the one before it.
     *
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps
     *
     * @return non-empty-list<Pipeline> leaf first
     */
    public static function chain(array $steps, ?Extractor $source = null): array
    {
        return Planner::cut(new Read($source ?? self::source()), $steps, 0);
    }

    /**
     * The last pipeline of chain(): the one a plan's root is.
     *
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps
     */
    public static function root(Extractor $source, array $steps): Pipeline
    {
        $chain = self::chain($steps, $source);

        return $chain[count($chain) - 1];
    }

    /**
     * @param list<JoinStep|Loader|Processor|BranchPipeline|TransactionGroup|Transformer> $steps
     */
    public static function plan(
        array $steps,
        ?Extractor $source = null,
        bool $countsOnly = false,
        bool $described = true,
    ): PhysicalPlan {
        $source ??= self::source();
        $chain = self::chain($steps, $source);
        $root = $chain[count($chain) - 1];

        return $described
            ? new PhysicalPlan($root, schema(), null, $countsOnly)
            : new PhysicalPlan(
                $root,
                null,
                DataDependentSchemaException::step('PipelinesMother', 'not described'),
                $countsOnly,
            );
    }

    /**
     * A parallel source of ten `id` rows in one batch.
     */
    public static function source(): RecordingSplittableExtractor
    {
        return new RecordingSplittableExtractor(schema(int_schema('id')), [RowsMother::sequentialIds(10)]);
    }
}
