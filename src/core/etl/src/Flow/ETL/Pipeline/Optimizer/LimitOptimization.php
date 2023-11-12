<?php declare(strict_types=1);

namespace Flow\ETL\Pipeline\Optimizer;

use Flow\ETL\Extractor\LimitableExtractor;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline;
use Flow\ETL\Pipeline\BatchingPipeline;
use Flow\ETL\Pipeline\CollectingPipeline;
use Flow\ETL\Pipeline\LocalSocketPipeline;
use Flow\ETL\Pipeline\NestedPipeline;
use Flow\ETL\Pipeline\ParallelizingPipeline;
use Flow\ETL\Pipeline\SynchronousPipeline;
use Flow\ETL\Pipeline\VoidPipeline;
use Flow\ETL\Row\Reference\ExpandResults;
use Flow\ETL\Transformer;
use Flow\ETL\Transformer\CallbackRowTransformer;
use Flow\ETL\Transformer\EntryExpressionEvalTransformer;
use Flow\ETL\Transformer\EntryNameStyleConverterTransformer;
use Flow\ETL\Transformer\KeepEntriesTransformer;
use Flow\ETL\Transformer\LimitTransformer;
use Flow\ETL\Transformer\RemoveEntriesTransformer;
use Flow\ETL\Transformer\RenameAllCaseTransformer;
use Flow\ETL\Transformer\RenameEntryTransformer;
use Flow\ETL\Transformer\RenameStrReplaceAllEntriesTransformer;

final class LimitOptimization implements Optimization
{
    /**
     * @psalm-suppress DeprecatedClass
     */
    private array $nonExpandingPipelines = [
        SynchronousPipeline::class,
        CollectingPipeline::class,
        BatchingPipeline::class,
        LocalSocketPipeline::class,
        NestedPipeline::class,
        ParallelizingPipeline::class,
        VoidPipeline::class,
    ];

    private array $nonExpandingTransformers = [
        CallbackRowTransformer::class,
        EntryExpressionEvalTransformer::class,
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
            if ($pipelineElement instanceof EntryExpressionEvalTransformer) {
                if ($pipelineElement->expression instanceof ExpandResults) {
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
