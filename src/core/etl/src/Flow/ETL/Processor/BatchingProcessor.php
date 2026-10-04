<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

/**
 * Re-batches rows into fixed-size batches.
 */
final readonly class BatchingProcessor implements Processor
{
    /**
     * @param int<1, max> $size
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public int $size,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison
        if ($this->size <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->size);
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
        $pending = null;
        $schema = null;

        while ($rows->valid()) {
            $batch = $rows->current();
            $schema ??= $batch->schema();

            if (!$batch->isEmpty()) {
                $pending = $pending === null
                    ? $batch->matchTo($schema, $context->backend())
                    : $pending->concat($context->backend(), $batch->matchTo($schema, $context->backend()));

                while ($pending->count() >= $this->size) {
                    $signal = yield $pending->slice(0, $this->size);

                    if ($signal === Signal::STOP) {
                        $rows->send(Signal::STOP);

                        return;
                    }

                    $pending = $pending->drop($this->size);
                }
            }

            $rows->next();
        }

        if ($pending !== null && !$pending->isEmpty()) {
            yield $pending;
        }
    }
}
