<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Executor;
use Flow\ETL\Executor\Pipeline;
use Flow\ETL\Executor\Segments;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Generator;

use function Flow\ETL\DSL\flow_context;

/**
 * Runs hand-built Segments as the one pipeline of a plan - what a test of a single Processor needs
 * from the Executor.
 */
final class ExecutedSegments
{
    /**
     * @return Generator<int, Rows>
     */
    public static function of(Segments $segments, ?FlowContext $context = null): Generator
    {
        return (new Executor())->executePipeline(new Pipeline(0, $segments, $context ?? flow_context()));
    }
}
