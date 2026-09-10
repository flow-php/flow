<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function count;

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
        /** @var array<Row> $buffer */
        $buffer = [];
        $currentValue = null;
        $hasValue = false;
        $schema = null;

        while ($rows->valid()) {
            $batch = $rows->current();
            $schema ??= $batch->schema();

            foreach ($batch as $row) {
                $value = $row->get($this->column);

                if (!$hasValue) {
                    $currentValue = $value;
                    $hasValue = true;
                }

                if ($value !== $currentValue) {
                    if ($this->minSize === null || count($buffer) >= $this->minSize) {
                        if ($buffer !== []) {
                            $signal = yield new Rows($schema, ...$buffer);

                            if ($signal === Signal::STOP) {
                                $rows->send(Signal::STOP);

                                return;
                            }

                            $buffer = [];
                        }
                    }
                    $currentValue = $value;
                }

                $buffer[] = $row;
            }

            $rows->next();
        }

        if ($buffer !== []) {
            yield new Rows($schema ?? new Schema(), ...$buffer);
        }
    }
}
