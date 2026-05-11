<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

/**
 * Groups rows into batches by column value.
 *
 * Assumes data is pre-sorted by the batching column. When the column value changes,
 * a new batch is started.
 *
 * @internal
 */
final readonly class BatchingByProcessor implements Processor
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private Reference $column,
        private ?int $minSize = null,
    ) {
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
        }
    }

    public function process(\Generator $rows, FlowContext $context): \Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];
        $currentValue = null;
        $hasValue = false;

        foreach ($rows as $batch) {
            /** @var Rows $batch */
            foreach ($batch as $row) {
                $value = $row->valueOf($this->column);

                if (!$hasValue) {
                    $currentValue = $value;
                    $hasValue = true;
                }

                if ($value !== $currentValue) {
                    if ($this->minSize === null || \count($buffer) >= $this->minSize) {
                        if ($buffer !== []) {
                            yield new Rows(...$buffer);
                            $buffer = [];
                        }
                    }
                    $currentValue = $value;
                }

                $buffer[] = $row;
            }
        }

        if ($buffer !== []) {
            yield new Rows(...$buffer);
        }
    }
}
