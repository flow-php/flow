<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Schema;
use Generator;

final readonly class PassThroughProcessor implements Processor
{
    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        yield from $rows;
    }
}
