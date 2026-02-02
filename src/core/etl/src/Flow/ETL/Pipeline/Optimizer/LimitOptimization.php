<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\{Loader, Pipeline, Processor, Transformer};
use Flow\ETL\Processor\{BatchingProcessor, CollectingProcessor, VoidProcessor};
use Flow\ETL\Transformer\{CallbackRowTransformer,
    DropEntriesTransformer,
    EntryNameStyleConverterTransformer,
    LimitTransformer,
    RenameAllCaseTransformer,
    RenameEachEntryTransformer,
    RenameEntryTransformer,
    RenameStrReplaceAllEntriesTransformer,
    ScalarFunctionTransformer,
    SelectEntriesTransformer};

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
        EntryNameStyleConverterTransformer::class,
        SelectEntriesTransformer::class,
        DropEntriesTransformer::class,
        RenameAllCaseTransformer::class,
        RenameEachEntryTransformer::class,
        RenameEntryTransformer::class,
        RenameStrReplaceAllEntriesTransformer::class,
        LimitTransformer::class,
    ];

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        if (!$element instanceof LimitTransformer) {
            return false;
        }

        if (!$pipeline->extractor() instanceof LimitableExtractor) {
            return false;
        }

        return $this->hasOnlyNonExpandingSteps($pipeline);
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        /** @var LimitableExtractor $extractor */
        $extractor = $pipeline->extractor();

        if ($extractor->isLimited()) {
            return $pipeline->add($element);
        }

        if ($element instanceof LimitTransformer && !\count($pipeline->stages()->steps())) {
            $extractor->changeLimit($element->limit);

            return $pipeline;
        }

        foreach ($pipeline->stages()->steps() as $pipelineElement) {
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

    private function hasOnlyNonExpandingSteps(Pipeline $pipeline) : bool
    {
        foreach ($pipeline->stages()->steps() as $step) {
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
