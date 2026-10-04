<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\BoundStep;
use Flow\ETL\Column\ComparableValues;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Processor;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_slice;

/**
 * Groups rows into batches by column value.
 *
 * Assumes data is pre-sorted by the batching column. When the column value changes,
 * a new batch is started.
 */
final readonly class BatchingByProcessor implements Processor
{
    /**
     * @param null|int<1, max> $minSize
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        public Reference $column,
        public ?int $minSize = null,
    ) {
        // @mago-ignore analysis:invalid-operand,impossible-condition,redundant-comparison,redundant-logical-operation
        if ($this->minSize !== null && $this->minSize <= 0) {
            throw new InvalidArgumentException('Minimum batch size must be greater than 0, given: ' . $this->minSize);
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
        /** @var list<Rows> $parts */
        $parts = [];
        $buffered = 0;
        $currentValue = null;
        $hasValue = false;
        $comparable = new ComparableValues();
        $schema = null;

        while ($rows->valid()) {
            $schema ??= $rows->current()->schema();
            $batch = $rows->current()->matchTo($schema, $context->backend());
            $start = 0;

            if ($batch->isEmpty()) {
                $rows->next();

                continue;
            }

            // @mago-ignore analysis:mixed-assignment
            foreach ($comparable->equality($batch->column($this->column->base())) as $i => $value) {
                if (!$hasValue) {
                    // @mago-ignore analysis:mixed-assignment
                    $currentValue = $value;
                    $hasValue = true;
                }

                if ($value !== $currentValue) {
                    $size = $buffered + $i - $start;

                    if (($this->minSize === null || $size >= $this->minSize) && $size > 0) {
                        if ($i > $start) {
                            $parts[] = $batch->slice($start, $i - $start);
                        }

                        $signal = yield $parts[0]->concat($context->backend(), ...array_slice($parts, 1));

                        if ($signal === Signal::STOP) {
                            $rows->send(Signal::STOP);

                            return;
                        }

                        $parts = [];
                        $buffered = 0;
                        $start = $i;
                    }

                    // @mago-ignore analysis:mixed-assignment
                    $currentValue = $value;
                }
            }

            if ($start < $batch->count()) {
                $parts[] = $batch->slice($start, $batch->count() - $start);
                $buffered += $batch->count() - $start;
            }

            $rows->next();
        }

        if ($parts !== []) {
            yield $parts[0]->concat($context->backend(), ...array_slice($parts, 1));
        }
    }
}
