<?php

declare(strict_types=1);

namespace Flow\ETL\Processor;

use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\BoundStep;
use Flow\ETL\Processor;
use Flow\ETL\Row;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function max;

/**
 * Buffers all rows and sorts them in memory. Registered by SortSteps when the sort algorithm is
 * memory_sort().
 *
 * @internal
 */
final readonly class MemorySortProcessor implements Processor
{
    public function __construct(
        private References $refs,
        private ?Schema $declared = null,
    ) {}

    public function process(Generator $rows, FlowContext $context): Generator
    {
        /** @var array<Row> $buffer */
        $buffer = [];
        $maxSize = 1;

        $schema = null;

        foreach ($rows as $batch) {
            $schema ??= $batch->schema();
            if ($batch->empty()) {
                continue;
            }

            $maxSize = max($batch->count(), $maxSize);

            foreach ($batch->all() as $row) {
                $buffer[] = $row;
            }
        }

        yield from (new Rows($this->declared ?? $schema ?? new Schema(), ...$buffer))
            ->sortBy(...$this->refs->all())
            ->chunks($maxSize);
    }

    public function bind(Schema $input): BoundStep
    {
        return new BoundStep(new self($this->refs, $input), $input);
    }
}
