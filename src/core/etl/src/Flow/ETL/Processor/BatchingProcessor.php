<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\{FlowContext, Processor, Row, Rows};

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
    public function __construct(private int $size)
    {
        if ($this->size <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->size);
        }
    }

    public function process(\Generator $rows, FlowContext $context) : \Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];

        foreach ($rows as $batch) {
            /** @var Rows $batch */
            foreach ($batch as $row) {
                $buffer[] = $row;

                if (\count($buffer) >= $this->size) {
                    yield new Rows(...\array_splice($buffer, 0, $this->size));
                }
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }
    }
}
