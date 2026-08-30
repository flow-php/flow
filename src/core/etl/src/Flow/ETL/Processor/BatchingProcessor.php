<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
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

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];

        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();

            foreach ($batch as $row) {
                $buffer[] = $row;

                if (count($buffer) >= $this->size) {
                    yield new Rows($schema, ...array_splice($buffer, 0, $this->size));
                }
            }
        }

        if ($buffer !== []) {
            yield new Rows($schema ?? new Schema(), ...$buffer);
        }
    }
}
