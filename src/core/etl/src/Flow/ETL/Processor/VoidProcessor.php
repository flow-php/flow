<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;

/**
 * Discards all rows and yields an empty batch.
 *
 * @internal
 */
final readonly class VoidProcessor implements Processor
{
    public function process(\Generator $rows, FlowContext $context): \Generator
    {
        foreach ($rows as $_batch) {
        }

        yield new Rows();
    }
}
