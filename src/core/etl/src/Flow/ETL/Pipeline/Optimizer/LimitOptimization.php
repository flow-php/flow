<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Processor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\DropEntriesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RenameEachEntryTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\ScalarFunctionTransformer;
use Flow\ETL\Transformer\SelectEntriesTransformer;

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
        CallbackRowTransformer::class,
        ScalarFunctionTransformer::class,
        SelectEntriesTransformer::class,
        DropEntriesTransformer::class,
        RenameEachEntryTransformer::class,
        RenameEntryTransformer::class,
        LimitTransformer::class,
    ];

    public function isFor(Loader|Transformer $element, Pipeline $pipeline): bool
    {
        if (!$element instanceof LimitTransformer) {
            return false;
        }

        if (!$pipeline->extractor() instanceof LimitableExtractor) {
            return false;
        }

        return $this->hasOnlyNonExpandingSteps($pipeline);
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline): Pipeline
    {
        /** @var LimitableExtractor $extractor */
        $extractor = $pipeline->extractor();

        if ($extractor->isLimited()) {
            return $pipeline->add($element);
        }

        if ($element instanceof LimitTransformer && !\count($pipeline->segments()->steps())) {
            $extractor->changeLimit($element->limit);

            return $pipeline;
        }

        foreach ($pipeline->segments()->steps() as $pipelineElement) {
            if ($pipelineElement instanceof ScalarFunctionTransformer) {
                if ($pipelineElement->function instanceof ExpandResults) {
                    break;
                }
            }

            if (!\in_array($pipelineElement::class, $this->nonExpandingTransformers, true)) {
                break;
            }

            if ($element instanceof LimitTransformer) {
                $extractor->changeLimit($element->limit);

                return $pipeline;
            }
        }

        return $pipeline->add($element);
    }

    private function hasOnlyNonExpandingSteps(Pipeline $pipeline): bool
    {
        foreach ($pipeline->segments()->steps() as $step) {
            if ($step instanceof Processor) {
                $isNonExpanding = false;

                foreach ($this->nonExpandingProcessors as $nonExpandingProcessor) {
                    if ($step instanceof $nonExpandingProcessor) {
                        $isNonExpanding = true;

                        break;
                    }
                }

                if (!$isNonExpanding) {
                    return false;
                }
            }
        }

        return true;
    }
}
