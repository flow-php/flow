<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\LimitPushDown;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\PruneEntriesTransformer;
use Flow\ETL\Transformer\RenameEachEntryTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

use function in_array;

/**
 * The limit operator stays in the plan, so a step missing from the allow-lists only costs the push; a step
 * that changes the row count on them would make the source read too few rows.
 */
final class LimitOptimization implements Optimization
{
    /**
     * Processors that don't expand the number of rows.
     *
     * @var array<int, class-string<Processor>>
     */
    private array $nonExpandingProcessors = [
        CollectingProcessor::class,
        BatchingProcessor::class,
        VoidProcessor::class,
    ];

    /**
     * @var array<int, class-string>
     */
    private array $nonExpandingTransformers = [
        ScalarFunctionTransformer::class,
        SelectEntriesTransformer::class,
        PruneEntriesTransformer::class,
        DropEntriesTransformer::class,
        RenameEachEntryTransformer::class,
        RenameEntryTransformer::class,
        LimitTransformer::class,
    ];

    public function isFor(Loader|Transformer $element, Pipeline $pipeline): bool
    {
        return $element instanceof LimitTransformer;
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline): Pipeline
    {
        $extractor = $pipeline->extractor();

        if (
            $element instanceof LimitTransformer
            && $extractor instanceof LimitPushDown
            && $this->hasOnlyNonExpandingSteps($pipeline)
        ) {
            // a hint only: the source may read less. The operator below still enforces the count.
            // Pushed into a copy the plan owns - the caller may read the same extractor again without a limit.
            $pushed = clone $extractor;
            $pushed->pushLimit($element->limit);
            $pipeline->replaceExtractor($pushed);
        }

        return $pipeline->add($element);
    }

    private function isNonExpandingStep(Loader|Processor|Transformer $step): bool
    {
        if (in_array($step::class, $this->nonExpandingTransformers, true)) {
            return true;
        }

        foreach ($this->nonExpandingProcessors as $nonExpandingProcessor) {
            if ($step instanceof $nonExpandingProcessor) {
                return true;
            }
        }

        return false;
    }

    private function hasOnlyNonExpandingSteps(Pipeline $pipeline): bool
    {
        foreach ($pipeline->segments()->steps() as $step) {
            if ($step instanceof ScalarFunctionTransformer && (new ExpandingFunctions())->in($step->function) !== []) {
                return false;
            }

            if (!$this->isNonExpandingStep($step)) {
                return false;
            }
        }

        return true;
    }
}
