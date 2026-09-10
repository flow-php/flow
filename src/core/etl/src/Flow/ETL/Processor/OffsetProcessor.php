<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
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
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Offset must be greater than or equal to 0, given: ' . $this->offset);
        }
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep($this, $input);
    }

    /**
     * @param Generator<int, Rows> $rows
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function process(Generator $rows, FlowContext $context): Generator
    {
        if ($this->offset === 0) {
            yield from $rows;

            return;
        }

        $skippedRows = 0;

        while ($rows->valid()) {
            /** @var Rows $batch */
            $batch = $rows->current();
            $currentBatchSize = $batch->count();
            $remainingToSkip = $this->offset - $skippedRows;

            if ($remainingToSkip >= $currentBatchSize) {
                $skippedRows += $currentBatchSize;
                $rows->next();

                continue;
            }

            if ($remainingToSkip > 0) {
                $batch = $batch->drop($remainingToSkip);
                $skippedRows += $remainingToSkip;
            }

            if ($batch->count() > 0) {
                $signal = yield $batch;

                if ($signal === Signal::STOP) {
                    $rows->send(Signal::STOP);

                    return;
                }
            }

            $rows->next();
        }
    }
}
