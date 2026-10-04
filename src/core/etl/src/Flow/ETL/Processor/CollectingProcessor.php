<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\FlowContext;
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
 */
final readonly class CollectingProcessor implements Processor
{
    public function __construct(
        public ?Schema $declared = null,
    ) {}

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($input), $input);
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        $collected = null;

        foreach ($rows as $batch) {
            // an empty part adds nothing, whatever its schema
            $collected = match (true) {
                $collected === null, $collected->isEmpty() => $batch,
                $batch->isEmpty() => $collected,
                default => $collected->concat($context->backend(), $batch),
            };
        }

        yield $collected ?? Rows::empty($this->declared ?? new Schema(), $context->backend());
    }
}
