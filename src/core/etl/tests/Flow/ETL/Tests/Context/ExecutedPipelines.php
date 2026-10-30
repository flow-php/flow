<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Runtime\Executor\Executor;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\Mother\PipelinesMother;
use Flow\ETL\Values\Rows;
use Generator;

/**
 * Runs hand-picked steps over a source as the pipelines of a plan - what a test of a single Processor needs from the
 * Executor.
 */
use function Flow\ETL\DSL\flow_context;

final class ExecutedPipelines
{
    /**
     * @param list<Loader|Processor|Transformer> $steps
     *
     * @return Generator<int, Rows>
     */
    public static function of(Extractor $source, array $steps, ?FlowContext $context = null): Generator
    {
        return (new Executor())->executePipeline(PipelinesMother::root($source, $steps), $context ?? flow_context());
    }
}
