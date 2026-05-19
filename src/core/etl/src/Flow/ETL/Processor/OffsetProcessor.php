<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Generator;

/**
 * Skips the first N rows.
 *
 * @internal
 */
final readonly class OffsetProcessor implements Processor
{
    /**
     * @param int<0, max> $offset
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private int $offset,
    ) {
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $this->offset);
        }
    }

    public function process(Generator $rows, FlowContext $context): Generator
    {
        if ($this->offset === 0) {
            yield from $rows;

            return;
        }

        $skippedRows = 0;

        foreach ($rows as $batch) {
            /** @var Rows $batch */
            $currentBatchSize = $batch->count();
            $remainingToSkip = $this->offset - $skippedRows;

            if ($remainingToSkip >= $currentBatchSize) {
                $skippedRows += $currentBatchSize;

                continue;
            }

            if ($remainingToSkip > 0) {
                $batch = $batch->drop($remainingToSkip);
                $skippedRows += $remainingToSkip;
            }

            if ($batch->count() > 0) {
                yield $batch;
            }
        }
    }
}
