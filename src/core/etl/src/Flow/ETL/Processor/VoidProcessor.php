<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Discards all rows and yields an empty batch.
 *
 * @internal
 */
final readonly class VoidProcessor implements Processor
{
    public function __construct(
        private ?Schema $declared = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($input), $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        foreach ($rows as $_batch) {
        }

        // void() drops rows, not columns
        yield new Rows($this->declared ?? new Schema());
    }
}
