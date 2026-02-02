<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\{FlowContext, Processor, Rows};

/**
 * Discards all rows and yields an empty batch.
 *
 * @internal
 */
final readonly class VoidProcessor implements Processor
{
    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        foreach ($rows as $batch) {
            // consume and discard
        }

        yield new Rows();
    }
}
