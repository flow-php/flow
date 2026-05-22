<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Generator;

/**
 * Collects all rows from the upstream generator into a single batch.
 *
 * This processor consumes the entire input generator and yields
 * all rows as a single Rows batch. Use with caution on large datasets
 * as it loads everything into memory.
 *
 * @internal
 */
final readonly class CollectingProcessor implements Processor
{
    public function process(Generator $rows, FlowContext $context): Generator
    {
        $collected = new Rows();

        foreach ($rows as $batch) {
            $collected = $collected->merge($batch);
        }

        yield $collected;
    }
}
