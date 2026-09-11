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
    public function __construct(
        private ?Schema $declared = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($input), $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $collected = null;

        foreach ($rows as $batch) {
            $collected = $collected === null ? $batch : $collected->merge($batch);
        }

        yield $collected ?? new Rows($this->declared ?? new Schema());
    }
}
