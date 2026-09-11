<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_splice;
use function count;

/**
 * Re-batches rows into fixed-size batches.
 *
 * @internal
 */
final readonly class BatchingProcessor implements Processor
{
    /**
     * @param int<1, max> $size
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private int $size,
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
        /** @var array<Row> $buffer */
        $buffer = [];

        $schema = null;

        // rows re-batched from batches under one schema already passed the gate under it - only a batch that
        // arrives under another schema needs the full gate to conform it to the first one
        $uniform = true;

        while ($rows->valid()) {
            $batch = $rows->current();
            $schema ??= $batch->schema();
            $uniform = $uniform && $batch->schema()->isSame($schema);

            foreach ($batch as $row) {
                $buffer[] = $row;

                if (count($buffer) >= $this->size) {
                    $chunk = array_splice($buffer, 0, $this->size);
                    $signal = yield $uniform ? Rows::trusted($schema, $chunk) : new Rows($schema, ...$chunk);

                    if ($signal === Signal::STOP) {
                        $rows->send(Signal::STOP);

                        return;
                    }
                }
            }

            $rows->next();
        }

        if ($buffer !== []) {
            $schema ??= new Schema();

            yield $uniform ? Rows::trusted($schema, $buffer) : new Rows($schema, ...$buffer);
        }
    }
}
