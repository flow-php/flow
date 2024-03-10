<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Function\ScalarFunction\ExpandResults;
use Flow\ETL\Pipeline\{BatchingPipeline, CollectingPipeline, NestedPipeline, SynchronousPipeline, VoidPipeline};
use Flow\ETL\Transformer\{CallbackRowTransformer, EntryNameStyleConverterTransformer, KeepEntriesTransformer, LimitTransformer, RemoveEntriesTransformer, RenameAllCaseTransformer, RenameEntryTransformer, RenameStrReplaceAllEntriesTransformer, ScalarFunctionTransformer};
use Flow\ETL\{Loader, Pipeline, Transformer};

final class LimitOptimization implements Optimization
{
    private array $nonExpandingPipelines = [
        SynchronousPipeline::class,
        CollectingPipeline::class,
        BatchingPipeline::class,
        NestedPipeline::class,
        VoidPipeline::class,
    ];

    private array $nonExpandingTransformers = [
        CallbackRowTransformer::class,
        ScalarFunctionTransformer::class,
        EntryNameStyleConverterTransformer::class,
        KeepEntriesTransformer::class,
        RemoveEntriesTransformer::class,
        RenameAllCaseTransformer::class,
        RenameEntryTransformer::class,
        RenameStrReplaceAllEntriesTransformer::class,
        LimitTransformer::class,
    ];

    public function isFor(Loader|Transformer $element, Pipeline $pipeline) : bool
    {
        return $element instanceof LimitTransformer
            && \in_array($pipeline::class, $this->nonExpandingPipelines, true)
            && $pipeline->source() instanceof LimitableExtractor;
    }

    public function optimize(Loader|Transformer $element, Pipeline $pipeline) : Pipeline
    {
        /** @var LimitableExtractor $extractor */
        $extractor = $pipeline->source();

        if ($extractor->isLimited()) {
            return $pipeline->add($element);
        }

        if ($element instanceof LimitTransformer && !\count($pipeline->pipes()->all())) {
            $extractor->changeLimit($element->limit);

            return $pipeline;
        }

        foreach ($pipeline->pipes()->all() as $pipelineElement) {
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
}
